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

package org.apache.spark.sql.test

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.StructType

import scala.util.Random

class ColumnarPluginTest extends QueryTest with SharedSparkSession {
  private val parquetFile1 = "parquet-1.parquet"

  object Mode extends Enumeration {
    val VANILLA_SPARK, VANILLA_SPARK_AND_ARROW_DATASOURCE, COLUMNAR_PLUGIN, COLUMNAR_PLUGIN_WITH_COLUMNAR_WINDOW = Value
  }

  private val MAX_DIRECT_MEMORY = 512 * 1024 * 1024
  private val MODE = Mode.VANILLA_SPARK
  private val tmpFolder = "/tmp/"

  private lazy val session = new SparkSession(new SparkContext("local[8]", "test-sql-context",
    sparkConf.set("spark.sql.testkey", "true"))) {

    SparkSession.setDefaultSession(this)
    SparkSession.setActiveSession(this)

    @transient
    override lazy val sessionState: SessionState = {
      new TestSQLSessionStateBuilder(this, None).build()
    }
  }

  override protected def sparkConf: SparkConf = {
    if (MODE == Mode.VANILLA_SPARK) {
      val conf = super.sparkConf
      conf
          .set("spark.executor.memory", "16g")
          .set("spark.memory.storageFraction", "0.1")
          //          .set("spark.memory.offHeap.size", String.valueOf(128 * 1024 * 1024))
          //          .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
          //          .set("spark.sql.codegen.wholeStage", "false")
          //          .set("spark.sql.sources.useV1SourceList", "")
          .set("spark.sql.columnar.tmp_dir", tmpFolder)
          //          .set("spark.sql.adaptive.enabled", "false")
          //          .set("spark.sql.columnar.sort.broadcastJoin", "true")
          .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
          .set("spark.executor.heartbeatInterval", "3600000")
          .set("spark.network.timeout", "3601s")
      //          .set("spark.oap.sql.columnar.preferColumnar", "false")
      //          .set("spark.sql.columnar.codegen.hashAggregate", "false")
      //          .set("spark.sql.columnar.sort", "false")
      //          .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      return conf
    }
    if (MODE == Mode.VANILLA_SPARK_AND_ARROW_DATASOURCE) {
      val conf = super.sparkConf
      conf
          .set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
          //          .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
          //          .set("spark.sql.codegen.wholeStage", "false")
          .set("spark.sql.sources.useV1SourceList", "")
          .set("spark.sql.columnar.tmp_dir", tmpFolder)
          //          .set("spark.sql.adaptive.enabled", "false")
          //          .set("spark.sql.columnar.sort.broadcastJoin", "true")
          .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
          .set("spark.executor.heartbeatInterval", "3600000")
          .set("spark.network.timeout", "3601s")
      //          .set("spark.oap.sql.columnar.preferColumnar", "false")
      //          .set("spark.sql.columnar.codegen.hashAggregate", "false")
      //          .set("spark.sql.columnar.sort", "false")
      //          .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      return conf
    }
    if (MODE == Mode.COLUMNAR_PLUGIN) {
      val conf = super.sparkConf
      conf.set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
          .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
          .set("spark.sql.codegen.wholeStage", "false")
          .set("spark.sql.sources.useV1SourceList", "")
          .set("spark.sql.columnar.tmp_dir", tmpFolder)
          .set("spark.sql.adaptive.enabled", "false")
          .set("spark.sql.columnar.sort.broadcastJoin", "true")
          .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
          .set("spark.executor.heartbeatInterval", "3600000")
          .set("spark.network.timeout", "3601s")
          .set("spark.oap.sql.columnar.preferColumnar", "true")
          .set("spark.sql.columnar.codegen.hashAggregate", "false")
          .set("spark.sql.columnar.sort", "false")
          .set("spark.sql.columnar.window", "false")
          .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      return conf
    }
    if (MODE == Mode.COLUMNAR_PLUGIN_WITH_COLUMNAR_WINDOW) {
      val conf = super.sparkConf
      conf.set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
          .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
          .set("spark.sql.codegen.wholeStage", "false")
          .set("spark.sql.sources.useV1SourceList", "")
          .set("spark.sql.columnar.tmp_dir", tmpFolder)
          .set("spark.sql.adaptive.enabled", "false")
          .set("spark.sql.columnar.sort.broadcastJoin", "true")
          .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
          .set("spark.executor.heartbeatInterval", "3600000")
          .set("spark.network.timeout", "3601s")
          .set("spark.oap.sql.columnar.preferColumnar", "true")
          .set("spark.sql.columnar.codegen.hashAggregate", "false")
          .set("spark.sql.columnar.sort", "false")
          .set("spark.sql.columnar.window", "true")
          .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      return conf
    }
    throw new IllegalStateException()
  }

  lazy val getOrCreateSession: SparkSession = {
    SparkSession.cleanupAnyExistingSession()
    session
  }

  override protected implicit def spark: SparkSession = {
    getOrCreateSession
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    import testImplicits._

    spark.read
        .json(Seq("{\"col1\": \"apple\", \"col2\": 100}", "{\"col1\": \"pear\", \"col2\": 200}",
          "{\"col1\": \"apple\", \"col2\": 350}", "{\"col1\": \"apple\", \"col2\": 200}")
            .toDS())
        .repartition(1)
        .write
        .mode("overwrite")
        .parquet(ColumnarPluginTest.locateResourcePath(parquetFile1))

    val path = ColumnarPluginTest.locateResourcePath(parquetFile1)
    spark.catalog.createTable("tab1", path, "arrow")

    spark.read.parquet(path).createOrReplaceTempView("tab2")
  }

  override def afterAll(): Unit = {
    ColumnarPluginTest.delete(ColumnarPluginTest.locateResourcePath(parquetFile1))
    super.afterAll()
  }

  private def readParquetOnce = {
    val path = ColumnarPluginTest.BIG_PARQUET_FILE
    val frame = spark.read.parquet(path)
    frame.explain()
    frame.foreach(r => {})
  }

  private def readArrowOnce = {
    val path = ColumnarPluginTest.BIG_PARQUET_FILE
    val frame = spark.read.format("arrow").load(path)
    frame.explain()
    frame.foreach(r => {})
  }

  ignore("regular read") {
    spark.sql("select * from tab1").show()
  }

  test("regular read - datasource v2") {
    spark.sql("select * from tab2").show()
  }

  test("simple columnar aggregation") {
    val sql = "select col1, sum(col2) from tab1 group by col1 limit 10"
    val df = spark.sql(sql)
    df.explain()
    df.show()
  }

  test("simple columnar aggregation 2") {
    val sql = "select col1, avg(col2) from tab1 group by col1 limit 10"
    val df = spark.sql(sql)
    df.explain()
    df.show()
  }

  test("columnar window performance - datagen") {

    val rdd = spark.sparkContext.range(1, 150000000).map { i =>
      Row(i, Random.nextInt(10), Random.nextInt(100), Random.nextInt(1000), Random.nextInt(10000), Random.nextInt(100000), "ID is " + i + ".")
    }

    val schema = new StructType()
        .add("id", "long")
        .add("value1", "int")
        .add("value2", "int")
        .add("value3", "int")
        .add("value4", "int")
        .add("value5", "int")
        .add("description", "string")

    val df = spark.sqlContext.createDataFrame(rdd, schema)

    df.write
        .mode("overwrite")
        .parquet("/root/Downloads/gen-parquet/window")
  }

  test("columnar window performance") {
    if (MODE == Mode.VANILLA_SPARK) {
      spark.catalog.createTable("datagen_window", "/root/Downloads/gen-parquet/window", "unsplitable-parquet")
    } else {
      spark.catalog.createTable("datagen_window", "/root/Downloads/gen-parquet/window", "arrow")
    }

    val df = spark.sql("select id, value1, value2, value3, value4, value5, rank() over (partition by value2 order by value5 desc) as rank from datagen_window limit 100")
    df.explain()
    df.show()
    Thread.sleep(3600000)
  }

  test("simple columnar window") {
    val sql = "select col1, sum(col2) over (partition by col1) from tab1"
    val df = spark.sql(sql)
    df.explain()
    df.show()
  }

  test("simple columnar window 2") {
    val sql = "select col1, col2, rank() over (partition by col1 order by col2 desc) from tab1"
    val df = spark.sql(sql)
    df.explain()
    df.show()
  }

  test("simple columnar window 3") {
    val sql = "select col1, col2, rank() over (order by col1 desc, col2 desc) from tab1"
    val df = spark.sql(sql)
    df.explain()
    df.show()
  }

  test("datasource rdd - parquet") {
    readParquetOnce
  }

  test("datasource rdd - arrow") {
    readArrowOnce
  }

  test("bitwise_and") {
    val df = spark.sql("select col2 & 0 from tab1")
    val rows = df.collect()
    rows.foreach(row => {
      println(row)
    })
  }

  test("bitwise_or") {
    val df = spark.sql("select col2 | 0 from tab1")
    val rows = df.collect()
    rows.foreach(row => {
      println(row)
    })
  }

  test("bitwise_xor") {
    val df = spark.sql("select col2 ^ col2 from tab1")
    val rows = df.collect()
    rows.foreach(row => {
      println(row)
    })
  }

  test("bitwise_not") {
    val df = spark.sql("select ~ col2 from tab1")
    val rows = df.collect()
    rows.foreach(row => {
      println(row)
    })
  }

  test("shift_right") {
    val df = spark.sql("select shiftright(col2, 1) from tab1")
    val rows = df.collect()
    rows.foreach(row => {
      println(row)
    })
  }

  test("shift_left") {
    val df = spark.sql("select shiftleft(col2, 1) from tab1")
    val rows = df.collect()
    rows.foreach(row => {
      println(row)
    })
  }

  ignore("datasource rdd - comparison") {
    (0 until 10).foreach {
      _ => {
        readParquetOnce
      }
    }
    (0 until 10).foreach {
      _ => {
        readArrowOnce
      }
    }
  }
}


object ColumnarPluginTest {
  private val BIG_PARQUET_FILE = "/root/Downloads/lineitem-sample-big-nodict.parquet"

  private def locateResourcePath(resource: String): String = {
    classOf[ColumnarPluginTest].getClassLoader.getResource("")
        .getPath.concat(File.separator).concat(resource)
  }

  private def delete(path: String): Unit = {
    FileUtils.forceDelete(new File(path))
  }
}