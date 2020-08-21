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

import io.netty.util.internal.PlatformDependent
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.{ColumnarPluginTest, SharedSparkSession}
import org.apache.spark.SparkConf

class TPCDSTest extends QueryTest with SharedSparkSession {
  object Mode extends Enumeration {
    val VANILLA_SPARK, VANILLA_SPARK_AND_ARROW_DATASOURCE, COLUMNAR_PLUGIN, COLUMNAR_PLUGIN_WITH_COLUMNAR_WINDOW = Value
  }

  private val MAX_DIRECT_MEMORY = "6g"
  private val MODE = Mode.COLUMNAR_PLUGIN_WITH_COLUMNAR_WINDOW

  override protected def sparkConf: SparkConf = {
    if (MODE == Mode.VANILLA_SPARK) {
      val conf = super.sparkConf
      conf
//          .set("spark.memory.offHeap.size", String.valueOf(128 * 1024 * 1024))
//          .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
//          .set("spark.sql.codegen.wholeStage", "false")
//          .set("spark.sql.sources.useV1SourceList", "")
          .set("spark.sql.columnar.tmp_dir", "/tmp/")
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
          .set("spark.sql.columnar.tmp_dir", "/tmp/")
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
          .set("spark.sql.columnar.tmp_dir", "/tmp/")
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
          .set("spark.sql.autoBroadcastJoinThreshold", "1")
          .set("spark.unsafe.exceptionOnMemoryLeak", "false")
      return conf
    }
    throw new IllegalStateException()
  }


  override def beforeAll(): Unit = {
    super.beforeAll()
    import testImplicits._

    val tpcdsRoot = "/root/Downloads/tpcds_10"
    val files = new File(tpcdsRoot).listFiles()
    files.foreach(file => {
      println("Creating catalog table: " + file.getName)
      spark.catalog.createTable(file.getName, file.getAbsolutePath, if (MODE == Mode.VANILLA_SPARK) "parquet" else "arrow")
    })

  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("tpc-ds q12") {
    runTPCDSQuery(12)
  }

  test("tpc-ds q20") {
    runTPCDSQuery(20)
  }

  test("tpc-ds q36") {
    runTPCDSQuery(36)
  }

  // multiple order by columns
  test("tpc-ds q47") {
    runTPCDSQuery(47)
  }

  // unsupported rank type: decimal
  test("tpc-ds q49") {
    runTPCDSQuery(49)
  }

  // max, frame operations
  ignore("tpc-ds q51") {
    runTPCDSQuery(51)
  }

  test("tpc-ds q53") {
    runTPCDSQuery(53)
  }

  // multiple order by columns
  test("tpc-ds q57") {
    runTPCDSQuery(57)
  }

  test("tpc-ds q63") {
    runTPCDSQuery(63)
  }

  test("tpc-ds q67") {
    runTPCDSQuery(67)
  }

  test("tpc-ds q70") {
    runTPCDSQuery(70)
  }

  test("tpc-ds q86") {
    runTPCDSQuery(86)
  }

  test("tpc-ds q89") {
    runTPCDSQuery(89)
  }

  test("tpc-ds q98") {
    runTPCDSQuery(98)
  }

  test("tpc-ds single run") {
    LogManager.getRootLogger.setLevel(Level.WARN)
    runTPCDSQuery(12)
    println(PlatformDependent.usedDirectMemory())
    println(NativeMemoryPool.getDefault.getBytesAllocated)
    Thread.sleep(3600000L)
  }

  test("tpc-ds single bench") {
    LogManager.getRootLogger.setLevel(Level.WARN)
    val total = benchTPCDSQuery(67, 1, 3)
    println("total time nano: " + total)
    Thread.sleep(3600000L)
  }


  test("tpc-ds queries benchmark - window basic") {
    LogManager.getRootLogger.setLevel(Level.WARN)
    val windowCases = Array(
      12,
      20,
      36,
      44,
      47,
      49,
//      51,
      53,
      57,
      63,
      67,
      70,
      86,
      89,
      98)
    val totalTime = windowCases.foldLeft(0L) {
      (accumulator, caseId) =>
        accumulator + benchTPCDSQuery(caseId, 1, 3)
    }
    println("total time nano: " + totalTime)
    Thread.sleep(3600000L)
  }

  test("tpc-ds queries") {
    LogManager.getRootLogger.setLevel(Level.WARN)
    val windowCases = Array(12, 20, 36, 44, 47, 49,
      //      51,
      53, 57, 63, 67, 70, 86, 89, 98)
    val totalTime = windowCases.foldLeft(0L) {
      (accumulator, caseId) =>
        accumulator + benchTPCDSQuery(caseId, 0, 1)
    }
    println("total time nano: " + totalTime)
  }

  private def runTPCDSQuery(caseId: Int): Unit = {
    val path = "tpcds/q" + caseId + ".sql";
    val absolute = TPCDSTest.locateResourcePath(path)
    val sql = FileUtils.readFileToString(new File(absolute), StandardCharsets.UTF_8)
    val df = spark.sql(sql)
    df.explain()
    df.show(100)
  }

  private def benchTPCDSQuery(caseId: Int, warmUpForEach: Int, runsForEach: Int): Long = {
    val path = "tpcds/q" + caseId + ".sql";
    val absolute = TPCDSTest.locateResourcePath(path)
    val sql = FileUtils.readFileToString(new File(absolute), StandardCharsets.UTF_8)
    (0 until warmUpForEach).foreach { _ =>
      println("Warming up case: " + caseId)
      val df = spark.sql(sql)
      val rowCount = df.collect().length
    }
    val prev = System.nanoTime()
    (0 until runsForEach).foreach { _ =>
      println("Executing case: " + caseId)
      val df = spark.sql(sql)
      val rowCount = df.collect().length
    }
    val cost = System.nanoTime() - prev
    cost
  }
}


object TPCDSTest {
  private val BIG_PARQUET_FILE = "/root/Downloads/lineitem-sample-big-nodict.parquet"

  private def locateResourcePath(resource: String): String = {
    classOf[ColumnarPluginTest].getClassLoader.getResource("")
        .getPath.concat(File.separator).concat(resource)
  }

  private def delete(path: String): Unit = {
    FileUtils.forceDelete(new File(path))
  }
}


