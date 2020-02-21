package com.intel.sparkColumnarPlugin.execution

import com.intel.sparkColumnarPlugin.expression._
import com.intel.sparkColumnarPlugin.vectorized._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.TaskContext

/**
 * A version of ProjectExec that adds in columnar support.
 */
class ColumnarProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
    extends ProjectExec(projectList, child) {

  override def supportsColumnar = true

  // Disable code generation
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions { iter =>
      val project = ColumnarProjection.create(projectList, child.output)
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

class ColumnarFilterExec(condition: Expression, child: SparkPlan)
    extends FilterExec(condition, child) {

  override def supportsColumnar = true

  // Disable code generation
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions { iter =>
      val filter = ColumnarFilter.create(condition, child.output)
      new CloseableColumnBatchIterator(iter.map(filter))
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
    return other.isInstanceOf[ColumnarFilterExec]
  }
}

case class ColumnarConditionProjectExec(condition: Expression, child: SparkPlan)
  extends UnaryExecNode with CodegenSupport with PredicateHelper {

  var projectList: Seq[NamedExpression] = _

  def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  val notNullAttributes = if (condition != null) {
    val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
      case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
      case _ => false
    }
    notNullPreds.flatMap(_.references).distinct.map(_.exprId)
  } else {
    null
  }
  override def output: Seq[Attribute] = if (projectList != null) {
    projectList.map(_.toAttribute)
  } else if (condition != null){
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  } else {
    child.output.map { a =>
      a
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def canEqual(that: Any): Boolean = false

  protected override def doExecute(): org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override def supportsColumnar = true

  // Disable code generation
  override def supportCodegen: Boolean = false

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of Input batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in ConditionProject process"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val numInputBatches = longMetric("numInputBatches")
    val procTime = longMetric("processTime")
    numOutputRows.set(0)
    numOutputBatches.set(0)
    numInputBatches.set(0)

    child.executeColumnar().mapPartitions { iter =>
      val condProj = ColumnarConditionProjector.create(
        condition,
        projectList,
        child.output,
        numInputBatches,
        numOutputBatches,
        numOutputRows,
        procTime)
      TaskContext
        .get()
        .addTaskCompletionListener[Unit]((tc: TaskContext) => {
          condProj.close()
        })
      new CloseableColumnBatchIterator(condProj.createIterator(iter))
    }
  }

  def addProjExprs(projectExprs: Seq[NamedExpression]): Unit = {
    projectList = projectExprs
  }

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[ColumnarConditionProjectExec]
  }
}
