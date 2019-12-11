package com.intel.sparkColumnarPlugin.expression

import io.netty.buffer.ArrowBuf
import java.util.ArrayList
import java.util.concurrent.TimeUnit._
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
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
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
    resultExpressions: Seq[NamedExpression],
    numInputBatches: SQLMetric,
    numOutputBatches: SQLMetric,
    numOutputRows: SQLMetric,
    aggrTime: SQLMetric)
    extends Logging {
  // build gandiva projection here.
  var elapseTime_make: Long = 0
  var rowId: Int = 0
  var processedNumRows: Int = 0

  logInfo(
    s"groupingExpressions: $groupingExpressions,\noriginalInputAttributes: $originalInputAttributes,\naggregateExpressions: $aggregateExpressions,\naggregateAttributes: $aggregateAttributes,\nresultExpressions: $resultExpressions")

  var resultTotalRows: Int = 0
  // 1. same size: inputField contains groupFields and aggrField, and output contains groupFields and aggrField
  // 2. different size: inputField contains groupFields and aggrField, and output contains aggrField
  // 3. different size: inputField contains groupFields and aggrField and other columns due to wholestagecodegen, and output contains groupFields and aggrField
  // 4. different size: inputField contains groupFields and aggrField and other columns due to wholestagecodegen, and output contains aggrField
  //
  val keyFieldList: List[Field] = groupingExpressions.toList.map(attr => {
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })

  val aggrFieldList: List[Field] = originalInputAttributes.toList.filter(attr => !ifIn(s"${attr.name}#${attr.exprId.id}", groupingExpressions)).map( expr => {
    val attr = getAttrFromExpr(expr)
    //val attr = BindReferences.bindReference(attrExpr, originalInputAttributes)
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })

  val inputFieldList = keyFieldList ::: aggrFieldList
  val outputFieldList: List[Field] = resultExpressions.toList.map( expr => {
    val attr = getAttrFromExpr(expr)
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })

  val groupExpression: List[ColumnarAggregateExpressionBase] = groupingExpressions.toList.map(field => {
    new ColumnarUniqueAggregateExpression().asInstanceOf[ColumnarAggregateExpressionBase]
  })

  val aggrExpression: List[ColumnarAggregateExpressionBase] = aggregateExpressions.toList.map(a => {
    new ColumnarAggregateExpression(
      a.aggregateFunction,
      a.mode,
      a.isDistinct,
      a.resultId).asInstanceOf[ColumnarAggregateExpressionBase]
  })

  // when zipping expression input and output, we should consider below scenario
  // 1. inputField contains groupFields and aggrField, and output contains groupFields and aggrField
  // 2. inputField contains aggrField, and output contains aggrField
  val (expressions, actualInputFieldList) = if (inputFieldList.size == outputFieldList.size) {
    (groupExpression ::: aggrExpression, inputFieldList)
  } else if (aggrFieldList.size == outputFieldList.size) {
    (aggrExpression, aggrFieldList)
  } else {
    throw new UnsupportedOperationException("ColumnarAggregation Unable to handle when result expression size either doesn't match groupingExpressions.size + aggrExpression.size or aggrExpression.size") 

  }
  val fieldPairList = (actualInputFieldList zip outputFieldList).map {
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
      expr.doColumnarCodeGen_ext((keyFieldList, actualInputFieldList, resultType, resultField))
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

  logInfo(s"Input Schema fields: $inputFieldList, aggregate input fields: $actualInputFieldList, result fields: $outputFieldList")

  var aggregator = new ExpressionEvaluator()
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

  def ifIn(name: String, attrList: Seq[NamedExpression]): Boolean = {
    val res = attrList.filter(attr => {
      name == s"${attr.name}#${attr.exprId.id}"
    })
    res.length > 0
  }

  def getAttrFromExpr(fieldExpr: Expression): AttributeReference = {
    fieldExpr match {
      case a: AggregateExpression =>
        getAttrFromExpr(a.aggregateFunction.children(0))
      case a: Cast =>
        getAttrFromExpr(a.child)
      case a: AttributeReference =>
        a
      case a: Alias =>
        getAttrFromExpr(a.child)
      case other =>
        throw new UnsupportedOperationException(s"makeStructField is unable to parse from $other (${other.getClass}).")
    }
  }

  def close(): Unit = {
    logInfo("ColumnarAggregation ExpressionEvaluator closed");
    aggregator.close()
    aggregator = null
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
      StructField(s"${attr.name}", attr.dataType, true)
    }).toArray)

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

      val finalColumnarBatch = new ColumnarBatch(resultColumnVectorList.map(v => v.asInstanceOf[ColumnVector]), finalResultRecordBatchList(0).getLength())
      releaseArrowRecordBatchList(finalResultRecordBatchList)
      finalColumnarBatch
    }
  }

  def createIterator(cbIterator: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      var cb: ColumnarBatch = null

      override def hasNext: Boolean = {
        cbIterator.hasNext
      }

      override def next(): ColumnarBatch = {
        val beforeAgg = System.nanoTime()
        var eval_elapse : Long = 0
        var finish_elapse : Long = 0
        while (cbIterator.hasNext) {
          if (cb != null) {
            cb.close()
            cb = null
          }
          cb = cbIterator.next()

          val beforeEval = System.nanoTime()
          if (cb.numRows > 0) {
            updateAggregationResult(cb)
            processedNumRows += cb.numRows
            numInputBatches += 1
          }
          eval_elapse += System.nanoTime() - beforeEval
        }
        val beforeFinish = System.nanoTime()
        val outputBatch = getAggregationResult()
        finish_elapse += System.nanoTime() - beforeFinish
        val elapse = System.nanoTime() - beforeAgg
        aggrTime.set(NANOSECONDS.toMillis(elapse))
        logInfo(s"HasgAggregate Completed, total processed ${numInputBatches.value} batches, took ${NANOSECONDS.toMillis(elapse)} ms handling one file(including fetching + processing), took ${NANOSECONDS.toMillis(eval_elapse)} ms doing evaluation, ${NANOSECONDS.toMillis(finish_elapse)} ms doing finish process.");
        numOutputBatches += 1
        numOutputRows += outputBatch.numRows
        if (cb != null) {
          cb.close()
          cb = null
        }
        outputBatch
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
      resultExpressions: Seq[NamedExpression],
      numInputBatches: SQLMetric,
      numOutputBatches: SQLMetric,
      numOutputRows: SQLMetric,
      aggrTime: SQLMetric): ColumnarAggregation = synchronized {
    columnarAggregation = new ColumnarAggregation(
      partIndex,
      groupingExpressions,
      originalInputAttributes,
      aggregateExpressions,
      aggregateAttributes,
      resultExpressions,
      numInputBatches,
      numOutputBatches,
      numOutputRows,
      aggrTime)
    columnarAggregation
  }

  def close(): Unit = {
    if (columnarAggregation != null) {
      columnarAggregation.close()
      columnarAggregation = null
    }
  }
}
