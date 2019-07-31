package com.intel.sparkColumnarPlugin.execution

import com.intel.sparkColumnarPlugin.expression._
import com.intel.sparkColumnarPlugin.vectorized._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * A version of ProjectExec that adds in columnar support.
 */
class ColumnarProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends ProjectExec(projectList, child) {

  override def supportsColumnar = true

  // Disable code generation
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions { iter =>
      val project = ColumnarProjection.create(projectList, child.output)
      project.initialize()
      new CloseableColumnBatchIterator(iter.map(project))
    }
  }

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[ColumnarProjectExec]
  }
}

