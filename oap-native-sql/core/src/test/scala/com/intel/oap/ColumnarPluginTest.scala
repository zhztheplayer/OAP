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

import java.io.File
import java.nio.charset.StandardCharsets

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.SparkConf

class ColumnarPluginTest extends QueryTest with SharedSparkSession {
  private val parquetFile1 = "parquet-1.parquet"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", String.valueOf(128 * 1024 * 1024))
        .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
        .set("spark.sql.codegen.wholeStage", "false")
        .set("spark.sql.sources.useV1SourceList", "")
        .set("spark.sql.columnar.tmp_dir", "/tmp/")
        .set("spark.sql.adaptive.enabled", "false")
        .set("spark.sql.columnar.sort.broadcastJoin", "true")
        .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
        .set("spark.executor.heartbeatInterval", "3600000")
        .set("spark.network.timeout", "3601s")
        .set("spark.oap.sql.columnar.preferColumnar", "false")
        .set("spark.sql.columnar.codegen.hashAggregate", "false")
        .set("spark.sql.columnar.sort", "false")
//        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    conf
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