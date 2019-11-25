package com.intel.sparkColumnarPlugin.expression

import io.netty.buffer.ArrowBuf
import java.util.ArrayList
import java.util.concurrent.TimeUnit
import util.control.Breaks._

import com.intel.sparkColumnarPlugin.vectorized.ExpressionEvaluator
import com.google.common.collect.Lists
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized._
import org.apache.spark.sql.execution.vectorized.ArrowWritableColumnVector
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.TaskContext

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.ArrowType

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.List
import scala.collection.mutable.ArrayBuffer

class ColumnarAggregation(
    partIndex: Int,
    groupingExpressions: Seq[NamedExpression],
    originalInputAttributes: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression])
    extends Logging {
  // build gandiva projection here.
  var elapseTime_make: Long = 0
  var rowId: Int = 0
  var processedNumRows: Int = 0

  logInfo(
    s"groupingExpressions: $groupingExpressions,\noriginalInputAttributes: $originalInputAttributes,\naggregateExpressions: $aggregateExpressions,\naggregateAttributes: $aggregateAttributes,\nresultExpressions: $resultExpressions")

  var resultTotalRows: Int = 0

  val keyFieldList: List[Field] = groupingExpressions
    .map(expr => {
      Field.nullable(s"${expr.name}", CodeGeneration.getResultType(expr.dataType))
    })
    .toArray
    .toList

  val aggregateColumnarExpressions = (aggregateExpressions zip aggregateAttributes)
    .map {
      case (expr, attr) =>
        val a = expr.asInstanceOf[AggregateExpression]
        val child = a.aggregateFunction.children.toList.head
        val field = getFieldByName(child, attr.dataType)
        new ColumnarAggregateExpression(
          field,
          a.aggregateFunction,
          a.mode,
          a.isDistinct,
          a.resultId)
    }
    .asInstanceOf[List[ColumnarAggregateExpressionBase]]

  /*resultType field will not be use in HashAggregate, but since we need one to make Gandiva Expression, fake one here.*/
  val resultType = CodeGeneration.getResultType()
  val expressions: List[ColumnarAggregateExpressionBase] = if (keyFieldList.length > 0) {
    val uniqueExpressions =
      keyFieldList.map(field => new ColumnarUniqueAggregateExpression(field))
    uniqueExpressions ::: aggregateColumnarExpressions
  } else {
    aggregateColumnarExpressions
  }

  val fieldTypesList: List[Field] = if (keyFieldList.length > 0) {
    keyFieldList ::: aggregateColumnarExpressions.map({ expr =>
      expr.getField
    })
  } else {
    aggregateColumnarExpressions.map({ expr =>
      expr.getField
    })
  }

  val gandivaExpressionTree: List[(ExpressionTree, Field, ExpressionTree)] =
    expressions
      .map {
        case expr => {
          val resultField =
            Field.nullable(s"result_${expr.getFieldName}", expr.getFieldType)
          val (node, finalNode) =
            expr.doColumnarCodeGen_ext((keyFieldList, fieldTypesList, resultType, resultField))
          if (node == null) {
            null
          } else {
            (
              TreeBuilder.makeExpression(node, resultField),
              resultField,
              TreeBuilder.makeExpression(finalNode, resultField))
          }
        }
      }
      .filter(_ != null)

  val resultFieldList = gandivaExpressionTree.map(_._2)
  logInfo(s"first evaluation fields: $fieldTypesList, second evaluation fields: $resultFieldList")

  val aggregator = new ExpressionEvaluator()
  val arrowSchema = new Schema(fieldTypesList.asJava)
  aggregator.build(
    arrowSchema,
    gandivaExpressionTree.map(_._1).asJava,
    gandivaExpressionTree.map(_._3).asJava)

  val resultArrowSchema = new Schema(resultFieldList.asJava)

  def getFieldByName(argExpr: Expression, dataType: DataType): Field = {
    val attr = argExpr match {
      case c: Cast =>
        c.child.asInstanceOf[AttributeReference]
      case a: AttributeReference =>
        a
      case other =>
        throw new UnsupportedOperationException(
          s"getFieldByName is unable parse arg name from $other (${other.getClass}).")
    }

    Field.nullable(s"${attr.name}", CodeGeneration.getResultType(dataType))
  }

  def close(): Unit = {
    aggregator.close()
  }

  def updateAggregationResult(columnarBatch: ColumnarBatch): Unit = {
    val inputRecordBatch = createArrowRecordBatch(columnarBatch)
    val resultRecordBatchList = aggregator.evaluate(inputRecordBatch)
    releaseArrowRecordBatch(inputRecordBatch)
    releaseArrowRecordBatchList(resultRecordBatchList)
  }

  def getAggregationResult(): ColumnarBatch = {
    logInfo("getAggregationResult")
    val resultSchema = ArrowUtils.fromArrowSchema(resultArrowSchema)
    if (processedNumRows == 0) {
      val resultColumnVectors =
        ArrowWritableColumnVector.allocateColumns(0, resultSchema).toArray
      return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
    } else {
      val finalResultRecordBatchList = aggregator.finish()
      if (finalResultRecordBatchList.size == 0) {
        val resultColumnVectors =
          ArrowWritableColumnVector.allocateColumns(0, resultSchema).toArray
        return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
      }
      val finalColumnarBatch = fromArrowRecordBatch(resultArrowSchema, finalResultRecordBatchList(0))
      logInfo(
        s"HashAggregate output columnar batch has numRows ${finalColumnarBatch.numRows}")
      releaseArrowRecordBatchList(finalResultRecordBatchList)
      finalColumnarBatch
    }
  }

  def createIterator(cbIterator: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      var cb: ColumnarBatch = null

      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        if (cb != null) {
          cb.close()
          cb = null
        }
      }

      override def hasNext: Boolean = {
        cbIterator.hasNext
      }

      override def next(): ColumnarBatch = {
        if (cb != null) {
          cb.close()
          cb = null
        }
        while (cbIterator.hasNext) {
          cb = cbIterator.next()
          if (cb.numRows > 0) {
            updateAggregationResult(cb)
            processedNumRows += cb.numRows
          }
        }
        getAggregationResult()
      }
    }
  }

  def createArrowRecordBatch(columnarBatch: ColumnarBatch): ArrowRecordBatch = {
    val fieldNodes = new ListBuffer[ArrowFieldNode]()
    val inputData = new ListBuffer[ArrowBuf]()
    val numRowsInBatch = columnarBatch.numRows()
    for (i <- 0 until columnarBatch.numCols()) {
      val inputVector =
        columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
      fieldNodes += new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount())
      inputData += inputVector.getValidityBuffer()
      inputData += inputVector.getDataBuffer()
    }
    new ArrowRecordBatch(numRowsInBatch, fieldNodes.toList.asJava, inputData.toList.asJava)
  }

  def fromArrowRecordBatch(schema: Schema, recordBatch: ArrowRecordBatch): ColumnarBatch = {
    val numRows = recordBatch.getLength();
    val resultColumnVectorList =
      ArrowWritableColumnVector.loadColumns(numRows, schema, recordBatch)
    new ColumnarBatch(resultColumnVectorList.map(_.asInstanceOf[ColumnVector]), numRows)
  }

  def releaseArrowRecordBatch(recordBatch: ArrowRecordBatch): Unit = {
    if (recordBatch != null)
      recordBatch.close()
  }

  def releaseArrowRecordBatchList(recordBatchList: Array[ArrowRecordBatch]): Unit = {
    recordBatchList.foreach({ recordBatch =>
      if (recordBatch != null)
        recordBatch.close()
    })
  }
}

object ColumnarAggregation {
  var columnarAggregation: ColumnarAggregation = _
  def create(
      partIndex: Int,
      groupingExpressions: Seq[NamedExpression],
      originalInputAttributes: Seq[Attribute],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      resultExpressions: Seq[NamedExpression]): ColumnarAggregation = synchronized {
    columnarAggregation = new ColumnarAggregation(
      partIndex,
      groupingExpressions,
      originalInputAttributes,
      aggregateExpressions,
      aggregateAttributes,
      resultExpressions)
    columnarAggregation
  }

  def close(): Unit = {
    if (columnarAggregation != null) {
      columnarAggregation.close()
    }
  }
}
