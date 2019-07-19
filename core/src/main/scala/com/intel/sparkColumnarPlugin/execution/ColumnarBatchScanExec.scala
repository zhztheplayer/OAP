package com.intel.sparkcolumnarPlugin.execution

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class ColumnarBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan) extends BatchScanExec(output, scan) {
  override def supportsColumnar(): Boolean = true
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val inputColumnarRDD = new ColumnarDataSourceRDD(sparkContext, partitions, readerFactory, true)
    inputColumnarRDD.map { r =>
      numOutputRows += r.numRows()
      r
    }
  }
}
