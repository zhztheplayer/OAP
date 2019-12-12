package com.intel.sparkColumnarPlugin.execution

import com.intel.sparkColumnarPlugin.expression._
import com.intel.sparkColumnarPlugin.vectorized._

import java.util.concurrent.TimeUnit._

import org.apache.spark.{SparkEnv, TaskContext, SparkContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.sql.execution._
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Columnar Based SortExec.
 */
class ColumnarSortExec(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan,
    testSpillFrequency: Int = 0)
extends SortExec(
  sortOrder,
  global,
  child,
  testSpillFrequency) {
  override def supportsColumnar = true

  // Disable code generation
  override def supportCodegen: Boolean = false

  override lazy val metrics = Map(
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in sort process"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val sortTime = longMetric("sortTime")
    child.executeColumnar().mapPartitions{ iter =>
      val hasInput = iter.hasNext
      val res = if (!hasInput) {
        Iterator.empty
      } else {
        val sorter = ColumnarSorter.create(sortOrder, child.output, sortTime)
        TaskContext.get().addTaskCompletionListener[Unit](_ => {
          sorter.close()
        })
        sorter.createIterator(iter)
      }
      res
    }
  }
}
