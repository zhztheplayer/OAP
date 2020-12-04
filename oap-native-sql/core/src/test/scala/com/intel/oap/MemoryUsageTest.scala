/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.oap

import java.io.{File, FileOutputStream, InputStreamReader, OutputStreamWriter}
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.{Scanner, StringTokenizer}
import java.util
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.intel.oap.TPCHTest.startRAMMonitorDaemon
import io.netty.util.internal.PlatformDependent
import io.prestosql.tpch.{Customer, CustomerGenerator, GenerateUtils, LineItem, LineItemGenerator, Nation, NationGenerator, Order, OrderGenerator, Part, PartGenerator, PartSupplier, PartSupplierGenerator, Region, RegionGenerator, Supplier, SupplierGenerator}
import javax.imageio.ImageIO
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.{QueryTest, Row, SaveMode}
import org.apache.spark.sql.test.{ColumnarPluginTest, SharedSparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.types._
import org.jfree.chart.ChartFactory
import org.jfree.data.statistics.HistogramDataset
import org.jfree.data.time.DynamicTimeSeriesCollection
import org.jfree.data.xy.DefaultXYDataset

import scala.collection.mutable.ArrayBuffer

class MemoryUsageTest extends QueryTest with SharedSparkSession {

  private val MAX_DIRECT_MEMORY = "6g"
  private val TPCH_WRITE_PATH = "/tmp/tpch-generated"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
        .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
        .set("spark.sql.codegen.wholeStage", "false")
        .set("spark.sql.sources.useV1SourceList", "")
        .set("spark.sql.columnar.tmp_dir", "/tmp/")
        .set("spark.sql.adaptive.enabled", "false")
        .set("spark.sql.columnar.sort.broadcastJoin", "true")
        .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
        .set("spark.executor.heartbeatInterval", "3600000")
        .set("spark.network.timeout", "3601s")
        .set("spark.oap.sql.columnar.preferColumnar", "true")
        .set("spark.sql.columnar.codegen.hashAggregate", "false")
        .set("spark.sql.columnar.sort", "true")
        .set("spark.sql.columnar.window", "true")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        //          .set("spark.sql.autoBroadcastJoinThreshold", "1")
        .set("spark.unsafe.exceptionOnMemoryLeak", "false")
        .set("spark.sql.columnar.sort.broadcast.cache.timeout", "600")
    return conf
  }


  override def beforeAll(): Unit = {
    super.beforeAll()
    // gen tpc-h data
    val scale = 1.0D
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    // lineitem
    def lineItemGenerator = { () =>
      new LineItemGenerator(scale, 1, 1)
    }

    def lineItemSchema = {
      StructType(Seq(
        StructField("l_orderkey", LongType),
        StructField("l_partkey", LongType),
        StructField("l_suppkey", LongType),
        StructField("l_linenumber", IntegerType),
        StructField("l_quantity", LongType),
        StructField("l_extendedprice", DoubleType),
        StructField("l_discount", DoubleType),
        StructField("l_tax", DoubleType),
        StructField("l_returnflag", StringType),
        StructField("l_linestatus", StringType),
        StructField("l_commitdate", DateType),
        StructField("l_receiptdate", DateType),
        StructField("l_shipinstruct", StringType),
        StructField("l_shipmode", StringType),
        StructField("l_comment", StringType),
        StructField("l_shipdate", DateType)
      ))
    }

    def lineItemParser: LineItem => Row =
      lineItem =>
        Row(
          lineItem.getOrderKey,
          lineItem.getPartKey,
          lineItem.getSupplierKey,
          lineItem.getLineNumber,
          lineItem.getQuantity,
          lineItem.getExtendedPrice,
          lineItem.getDiscount,
          lineItem.getTax,
          lineItem.getReturnFlag,
          lineItem.getStatus,
          Date.valueOf(GenerateUtils.formatDate(lineItem.getCommitDate)),
          Date.valueOf(GenerateUtils.formatDate(lineItem.getReceiptDate)),
          lineItem.getShipInstructions,
          lineItem.getShipMode,
          lineItem.getComment,
          Date.valueOf(GenerateUtils.formatDate(lineItem.getShipDate))
        )

    // customer
    def customerGenerator = { () =>
      new CustomerGenerator(scale, 1, 1)
    }

    def customerSchema = {
      StructType(Seq(
        StructField("c_custkey", LongType),
        StructField("c_name", StringType),
        StructField("c_address", StringType),
        StructField("c_nationkey", LongType),
        StructField("c_phone", StringType),
        StructField("c_acctbal", DoubleType),
        StructField("c_comment", StringType),
        StructField("c_mktsegment", StringType)
      ))
    }

    def customerParser: Customer => Row =
      customer =>
        Row(
          customer.getCustomerKey,
          customer.getName,
          customer.getAddress,
          customer.getNationKey,
          customer.getPhone,
          customer.getAccountBalance,
          customer.getComment,
          customer.getMarketSegment,
        )

    def rowCountOf[U](itr: java.lang.Iterable[U]): Long = {
      var cnt = 0L
      val iterator = itr.iterator
      while (iterator.hasNext) {
        iterator.next()
        cnt = cnt + 1
      }
      cnt
    }

    // orders
    def orderGenerator = { () =>
      new OrderGenerator(scale, 1, 1)
    }

    def orderSchema = {
      StructType(Seq(
        StructField("o_orderkey", LongType),
        StructField("o_custkey", LongType),
        StructField("o_orderstatus", StringType),
        StructField("o_totalprice", DoubleType),
        StructField("o_orderpriority", StringType),
        StructField("o_clerk", StringType),
        StructField("o_shippriority", IntegerType),
        StructField("o_comment", StringType),
        StructField("o_orderdate", DateType)
      ))
    }

    def orderParser: Order => Row =
      order =>
        Row(
          order.getOrderKey,
          order.getCustomerKey,
          String.valueOf(order.getOrderStatus),
          order.getTotalPrice,
          order.getOrderPriority,
          order.getClerk,
          order.getShipPriority,
          order.getComment,
          Date.valueOf(GenerateUtils.formatDate(order.getOrderDate))
        )

    // partsupp
    def partSupplierGenerator = { () =>
      new PartSupplierGenerator(scale, 1, 1)
    }

    def partSupplierSchema = {
      StructType(Seq(
        StructField("ps_partkey", LongType),
        StructField("ps_suppkey", LongType),
        StructField("ps_availqty", IntegerType),
        StructField("ps_supplycost", DoubleType),
        StructField("ps_comment", StringType)
      ))
    }

    def partSupplierParser: PartSupplier => Row =
      ps =>
        Row(
          ps.getPartKey,
          ps.getSupplierKey,
          ps.getAvailableQuantity,
          ps.getSupplyCost,
          ps.getComment
        )

    // supplier
    def supplierGenerator = { () =>
      new SupplierGenerator(scale, 1, 1)
    }

    def supplierSchema = {
      StructType(Seq(
        StructField("s_suppkey", LongType),
        StructField("s_name", StringType),
        StructField("s_address", StringType),
        StructField("s_nationkey", LongType),
        StructField("s_phone", StringType),
        StructField("s_acctbal", DoubleType),
        StructField("s_comment", StringType)
      ))
    }

    def supplierParser: Supplier => Row =
      s =>
        Row(
          s.getSupplierKey,
          s.getName,
          s.getAddress,
          s.getNationKey,
          s.getPhone,
          s.getAccountBalance,
          s.getComment
        )

    // nation
    def nationGenerator = { () =>
      new NationGenerator()
    }

    def nationSchema = {
      StructType(Seq(
        StructField("n_nationkey", LongType),
        StructField("n_name", StringType),
        StructField("n_regionkey", LongType),
        StructField("n_comment", StringType)
      ))
    }

    def nationParser: Nation => Row =
      nation =>
        Row(
          nation.getNationKey,
          nation.getName,
          nation.getRegionKey,
          nation.getComment
        )

    // part
    def partGenerator = { () =>
      new PartGenerator(scale, 1, 1)
    }

    def partSchema = {
      StructType(Seq(
        StructField("p_partkey", LongType),
        StructField("p_name", StringType),
        StructField("p_mfgr", StringType),
        StructField("p_type", StringType),
        StructField("p_size", IntegerType),
        StructField("p_container", StringType),
        StructField("p_retailprice", DoubleType),
        StructField("p_comment", StringType),
        StructField("p_brand", StringType)
      ))
    }

    def partParser: Part => Row =
      part =>
        Row(
          part.getPartKey,
          part.getName,
          part.getManufacturer,
          part.getType,
          part.getSize,
          part.getContainer,
          part.getRetailPrice,
          part.getComment,
          part.getBrand
        )

    // region
    def regionGenerator = { () =>
      new RegionGenerator()
    }

    def regionSchema = {
      StructType(Seq(
        StructField("r_regionkey", LongType),
        StructField("r_name", StringType),
        StructField("r_comment", StringType)
      ))
    }

    def regionParser: Region => Row =
      region =>
        Row(
          region.getRegionKey,
          region.getName,
          region.getComment
        )

    def generate[U](tableName: String, schema: StructType, gen: () => java.lang.Iterable[U],
        parser: U => Row): Unit = {
      spark.range(0, rowCountOf(gen.apply()), 1L, 1)
          .mapPartitions { itr =>
            val lineItem = gen.apply()
            val lineItemItr = lineItem.iterator()
            val rows = itr.map { _ =>
              val item = lineItemItr.next()
              parser(item)
            }
            rows
          }(RowEncoder(schema))
          .write
          .mode(SaveMode.Overwrite)
          .parquet(TPCH_WRITE_PATH + File.separator + tableName)
    }

    generate("lineitem", lineItemSchema, lineItemGenerator, lineItemParser)
    generate("customer", customerSchema, customerGenerator, customerParser)
    generate("orders", orderSchema, orderGenerator, orderParser)
    generate("partsupp", partSupplierSchema, partSupplierGenerator, partSupplierParser)
    generate("supplier", supplierSchema, supplierGenerator, supplierParser)
    generate("nation", nationSchema, nationGenerator, nationParser)
    generate("part", partSchema, partGenerator, partParser)
    generate("region", regionSchema, regionGenerator, regionParser)


    val files = new File(TPCH_WRITE_PATH).listFiles()
    files.foreach(file => {
      println("Creating catalog table: " + file.getName)
      spark.catalog.createTable(file.getName, file.getAbsolutePath, "arrow")
      try {
        spark.catalog.recoverPartitions(file.getName)
      } catch {
        case _: Throwable =>
      }
    })
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("test") {
    println("hello")
  }

  test("memory usage test long-run") {
    MemoryUsageTest.startRAMMonitorDaemon()
    LogManager.getRootLogger.setLevel(Level.WARN)
    (1 to 50).foreach { _ =>
      (1 to 22).foreach(i => runTPCHQuery(i))
    }
    println(PlatformDependent.usedDirectMemory())
    println(SparkMemoryUtils.globalMemoryPool().getBytesAllocated)
    println(SparkMemoryUtils.globalAllocator().getAllocatedMemory)
  }

  private def runTPCHQuery(caseId: Int): Unit = {
    val path = "tpch-queries/q" + caseId + ".sql";
    val absolute = MemoryUsageTest.locateResourcePath(path)
    val sql = FileUtils.readFileToString(new File(absolute), StandardCharsets.UTF_8)
    val df = spark.sql(sql)
    df.explain()
    df.show(100)
  }
}

object MemoryUsageTest {

  private def locateResourcePath(resource: String): String = {
    classOf[ColumnarPluginTest].getClassLoader.getResource("")
        .getPath.concat(File.separator).concat(resource)
  }
  private def delete(path: String): Unit = {
    FileUtils.forceDelete(new File(path))
  }

  private def getCurrentPIDRAMUsed(): Long = {
    val proc = Runtime.getRuntime.exec("ps -p " + getPID() + " -o rss")
    val in = new InputStreamReader(proc.getInputStream)
    val buff = new StringBuilder

    def scan: Unit = {
      while (true) {
        val ch = in.read()
        if (ch == -1) {
          return;
        }
        buff.append(ch.asInstanceOf[Char])
      }
    }
    scan
    in.close()
    val output = buff.toString()
    val scanner = new Scanner(output)
    scanner.nextLine()
    scanner.nextLine().toLong
  }

  private def getPID(): String = {
    val beanName = ManagementFactory.getRuntimeMXBean.getName
    return beanName.substring(0, beanName.indexOf('@'))
  }

  private def getOSRAMUsed(): Long = {
    val proc = Runtime.getRuntime.exec("free")
    val in = new InputStreamReader(proc.getInputStream)
    val buff = new StringBuilder

    def scan: Unit = {
      while (true) {
        val ch = in.read()
        if (ch == -1) {
          return;
        }
        buff.append(ch.asInstanceOf[Char])
      }
    }
    scan
    in.close()
    val output = buff.toString()
    val scanner = new Scanner(output)
    scanner.nextLine()
    val memLine = scanner.nextLine()
    val tok = new StringTokenizer(memLine)
    tok.nextToken()
    tok.nextToken()
    return tok.nextToken().toLong
  }


  private def startRAMMonitorDaemon(): Unit = {
    val counter = new AtomicInteger(0)
    val heapUsedBuffer = ArrayBuffer[Double]()
    val heapTotalBuffer = ArrayBuffer[Double]()
    val pidRamUsedBuffer = ArrayBuffer[Double]()
    val osRamUsedBuffer = ArrayBuffer[Double]()
    val indexBuffer = ArrayBuffer[Double]()

    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        val heapTotalBytes = Runtime.getRuntime.totalMemory()

        val i = counter.getAndIncrement()
        val pidRamUsed = getCurrentPIDRAMUsed()
        val osRamUsed = getOSRAMUsed()
        val heapUsed = (heapTotalBytes - Runtime.getRuntime.freeMemory()) / 1024L
        val heapTotal = heapTotalBytes / 1024L

        heapUsedBuffer.append(heapUsed)
        heapTotalBuffer.append(heapTotal)
        pidRamUsedBuffer.append(pidRamUsed)
        osRamUsedBuffer.append(osRamUsed)

        indexBuffer.append(i)


        if (i % 10 == 0) {
          val dataset = new DefaultXYDataset()
          dataset.addSeries("JVM Heap Used", Array(indexBuffer.toArray, heapUsedBuffer.toArray))
          dataset.addSeries("JVM Heap Total", Array(indexBuffer.toArray, heapTotalBuffer.toArray))
          dataset.addSeries("Process Res Total", Array(indexBuffer.toArray, pidRamUsedBuffer.toArray))
          dataset.addSeries("OS Res Total", Array(indexBuffer.toArray, osRamUsedBuffer.toArray))

          ImageIO.write(
            ChartFactory.createScatterPlot("RAM Usage History", "Up Time (Second)", "Memory Used (KB)", dataset)
                .createBufferedImage(512, 512),
          "png", new File("sample.png"))
        }
      }
    }, 0L, 1000L, TimeUnit.MILLISECONDS).get()
  }

  def main(args: Array[String]): Unit = {
    startRAMMonitorDaemon()
  }
}





