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

  val keyFieldList: List[Field] = groupingExpressions.toList.map(expr => {
    Field.nullable(s"${expr.name}#${expr.exprId.id}", CodeGeneration.getResultType(expr.dataType))
  })

  val inputFieldList: List[Field] = originalInputAttributes.toList.map( attr => {
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })

  val outputFieldList: List[Field] = (groupingExpressions.toList ::: aggregateAttributes.toList).map( attr => {
    //Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
    Field.nullable(s"${attr.name}", CodeGeneration.getResultType(attr.dataType))
  })

  val groupExpression: List[ColumnarAggregateExpressionBase] = keyFieldList.map(field => {
    new ColumnarUniqueAggregateExpression().asInstanceOf[ColumnarAggregateExpressionBase]
  })

  val aggrExpression: List[ColumnarAggregateExpressionBase] = aggregateExpressions.toList.map(a => {
    new ColumnarAggregateExpression(
      a.aggregateFunction,
      a.mode,
      a.isDistinct,
      a.resultId).asInstanceOf[ColumnarAggregateExpressionBase]
  })

  val expressions = groupExpression ::: aggrExpression
  val fieldPairList = (inputFieldList zip outputFieldList).map {
    case (inputField, outputField) => 
      (inputField, outputField)
  }
  (expressions zip fieldPairList).foreach {
    case (expr, fieldPair) =>
      expr.setField(fieldPair._1, fieldPair._2)
  }

  /*resultType field will not be use in HashAggregate, but since we need one to make Gandiva Expression, fake one here.*/
  val resultType = CodeGeneration.getResultType()

  val gandivaExpressionTree: List[(ExpressionTree, ExpressionTree)] = expressions.map( expr => {
    val resultField = expr.getResultField
    val (node, finalNode) =
      expr.doColumnarCodeGen_ext((keyFieldList, inputFieldList, resultType, resultField))
    if (node == null) {
      null
    } else if (finalNode == null) {
      (
        TreeBuilder.makeExpression(node, resultField),
        null)
    } else {
      (
        TreeBuilder.makeExpression(node, resultField),
        TreeBuilder.makeExpression(finalNode, resultField))
    }
  }).filter(_ != null)

  logInfo(s"first evaluation fields: $inputFieldList, second evaluation fields: $outputFieldList")

  val aggregator = new ExpressionEvaluator()
  val arrowSchema = new Schema(inputFieldList.asJava)
  if (keyFieldList.length > 0) {
  aggregator.build(
    arrowSchema,
    gandivaExpressionTree.map(_._1).asJava, true)
  } else {
  aggregator.build(
    arrowSchema,
    gandivaExpressionTree.map(_._1).asJava, gandivaExpressionTree.map(_._2).asJava)
  }


  def getAttrFromExpr(fieldExpr: Expression): (String, DataType) = {
    fieldExpr match {
      case c: Cast =>
        val attr = c.child.asInstanceOf[AttributeReference]
        (attr.name, c.dataType)
      case a: AttributeReference =>
        (a.name, a.dataType)
      case a: Alias =>
        getAttrFromExpr(a.child)
      case other =>
        throw new UnsupportedOperationException(s"makeStructField is unable to parse from $other (${other.getClass}).")
    }
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
    val resultSchema = StructType(resultExpressions.map(fieldExpr => {
      val attr = getAttrFromExpr(fieldExpr)
      StructField(s"${attr._1}", attr._2, true)
    }).toArray)
    logInfo(s"getAggregationResult resultSchema is ${resultSchema}")

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
      // Since grouping column may be removed in output, we need to check here.
      val recordBatchSchema = new Schema(outputFieldList.asJava)
      val resultColumnVectorList = fromArrowRecordBatch(recordBatchSchema, finalResultRecordBatchList(0))
      val resultNameList = resultExpressions.map(expr => getAttrFromExpr(expr)._1).toArray
      val filteredResultColumnVectorList = (expressions zip resultColumnVectorList).map{
        case (expr, arrowColumnVector) => {
          // we should only choose one inside resultExpression
          if (resultNameList contains expr.getResultFieldName) {
            arrowColumnVector.asInstanceOf[ColumnVector]
          } else {
            null
          }
      }}.filter(_ != null).toArray
      val finalColumnarBatch = new ColumnarBatch(filteredResultColumnVectorList, finalResultRecordBatchList(0).getLength())
      logInfo(
        s"HashAggregate output columnar batch has numRows ${finalColumnarBatch.numRows}, data is ${(0 until finalColumnarBatch.numCols).map(i => finalColumnarBatch.column(i).getUTF8String(0)).mkString(" ")}")
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

  def fromArrowRecordBatch(recordBatchSchema: Schema, recordBatch: ArrowRecordBatch): Array[ArrowWritableColumnVector] = {
    val numRows = recordBatch.getLength();
    ArrowWritableColumnVector.loadColumns(numRows, recordBatchSchema, recordBatch)
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
