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
    aggrTime: SQLMetric,
    elapseTime: SQLMetric)
    extends Logging {
  // build gandiva projection here.
  var elapseTime_make: Long = 0
  var rowId: Int = 0
  var processedNumRows: Int = 0
  val inputBatchHolder = new ListBuffer[ArrowRecordBatch]() 

  logInfo(
    s"groupingExpressions: $groupingExpressions,\noriginalInputAttributes: $originalInputAttributes,\naggregateExpressions: $aggregateExpressions,\naggregateAttributes: $aggregateAttributes,\nresultExpressions: $resultExpressions")

  var resultTotalRows: Int = 0
  // 1. same size: inputField contains groupFields and aggrField, and output contains groupFields and aggrField
  // 2. different size: inputField contains groupFields and aggrField, and output contains aggrField
  // 3. different size: inputField contains groupFields and aggrField and other columns due to wholestagecodegen, and output contains groupFields and aggrField
  // 4. different size: inputField contains groupFields and aggrField and other columns due to wholestagecodegen, and output contains aggrField
  //
  val inputAttributeSeq: AttributeSeq = originalInputAttributes
  val mode = aggregateExpressions(0).mode
  val aggregateBufferAttributes = {
    mode match {
      case Partial =>
        aggregateExpressions.map(_.aggregateFunction.children(0))
      case Final =>
        aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
        //aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate].evaluateExpression)
      case _ =>
        throw new UnsupportedOperationException(s"get aggregation Attribute failed support ${mode}")
    }
  }
  val keyInputList = groupingExpressions.map(expr => {
    BindReferences.bindReference(expr.asInstanceOf[Expression], originalInputAttributes)
  })
  val aggrInputList = aggregateBufferAttributes.map(expr => {
    var res = BindReferences.bindReference(expr.asInstanceOf[Expression], originalInputAttributes, true)
    if (!res.isInstanceOf[BoundReference]) {
      var ordinal = -1
      val a = getAttrFromExpr(expr)
      for (i <- 0 until originalInputAttributes.length) {
        if (a.name == originalInputAttributes(i).name) {
          ordinal = i
        }
      }
      if (ordinal == -1) {
        throw new UnsupportedOperationException(s"Couldn't find $a in ${originalInputAttributes.attrs.mkString("[", ",", "]")}")
      }
      res = BoundReference(ordinal, a.dataType, originalInputAttributes(ordinal).nullable) 
    }
    res
  })
  val keyInputOrdinalList = keyInputList.map(expr => getOrdinalFromExpr(expr)).toList
  val aggrInputOrdinalList = aggrInputList.map(expr => getOrdinalFromExpr(expr)).toList
  logInfo(s"keyInputList is ${keyInputOrdinalList}, aggrInputList is ${aggrInputOrdinalList}")

  var usedInputIndices: List[Int] = keyInputOrdinalList ::: aggrInputOrdinalList
  logInfo(s"indices is ${usedInputIndices}")

  logInfo(s"keyInputList is ${keyInputList}, aggrInputList is ${aggrInputList}, indices is ${usedInputIndices}")

  val keyFieldList: List[Field] = groupingExpressions.toList.map(attr => {
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })

  val aggrFieldList: List[Field] = aggregateExpressions.toList.map(expr => { 
    val attr = getAttrFromExpr(expr)
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })

  val inputFieldList = usedInputIndices.map(i => {
    val expr = originalInputAttributes(i)
    val attr = getAttrFromExpr(expr)
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })

  val outputFieldList: List[Field] = resultExpressions.toList.map( expr => {
    val attr = getAttrFromExpr(expr)
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })

  val groupExpression: List[ColumnarAggregateExpressionBase] = groupingExpressions.toList
    .filter(attr =>
      ifIn(s"${attr.name}#${attr.exprId.id}", resultExpressions))
    .map(field => 
      new ColumnarUniqueAggregateExpression().asInstanceOf[ColumnarAggregateExpressionBase]
  )

  val aggrExpression: List[ColumnarAggregateExpressionBase] = aggregateExpressions.toList.map(a => {
    new ColumnarAggregateExpression(
      a.aggregateFunction,
      a.mode,
      a.isDistinct,
      a.resultId).asInstanceOf[ColumnarAggregateExpressionBase]
  })

  val expressions = groupExpression ::: aggrExpression

  logInfo(s"inputFieldList is ${inputFieldList}, outputFieldList is ${outputFieldList}")
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

  def getOrdinalFromExpr(expr: Expression): Int = {
    expr match {
      case e: BoundReference =>
        e.ordinal
      case other =>
        throw new UnsupportedOperationException(s"getOrdinalFromExpr is unable to parse from $other (${other.getClass}).")
    }
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
        throw new UnsupportedOperationException(s"getAttrFromExpr is unable to parse from $other (${other.getClass}).")
    }
  }

  def close(): Unit = {
    logInfo("ColumnarAggregation ExpressionEvaluator closed");
    aggregator.close()
    aggregator = null
    ConverterUtils.releaseArrowRecordBatchList(inputBatchHolder.toArray)
  }

  def updateAggregationResult(columnarBatch: ColumnarBatch): Unit = {
    val cols = usedInputIndices.map(i => {
      columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
    })
    val inputRecordBatch: ArrowRecordBatch = ConverterUtils.createArrowRecordBatch(columnarBatch.numRows, cols)
    val resultRecordBatchList = aggregator.evaluate(inputRecordBatch)
   
    inputBatchHolder += inputRecordBatch
    ConverterUtils.releaseArrowRecordBatch(inputRecordBatch)
    ConverterUtils.releaseArrowRecordBatchList(resultRecordBatchList)
  }

  def getAggregationResult(): ColumnarBatch = {
    val resultSchema = new Schema(outputFieldList.asJava)
    val resultStructType = ArrowUtils.fromArrowSchema(resultSchema)
    if (processedNumRows == 0) {
      val resultColumnVectors =
        ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
      return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
    } else {
      val finalResultRecordBatchList = aggregator.finish()
      if (finalResultRecordBatchList.size == 0) {
        val resultColumnVectors =
          ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
        return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
      }
      // Since grouping column may be removed in output, we need to check here.
      val resultColumnVectorList = ConverterUtils.fromArrowRecordBatch(resultSchema, finalResultRecordBatchList(0))

      val finalColumnarBatch = new ColumnarBatch(resultColumnVectorList.map(v => v.asInstanceOf[ColumnVector]), finalResultRecordBatchList(0).getLength())
      ConverterUtils.releaseArrowRecordBatchList(finalResultRecordBatchList)
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
          }
          numInputBatches += 1
          eval_elapse += System.nanoTime() - beforeEval
        }
        logInfo(s"start to do Final Aggregation")
        val beforeEval = System.nanoTime()
        val outputBatch = getAggregationResult()
        eval_elapse += System.nanoTime() - beforeEval
        val elapse = System.nanoTime() - beforeAgg
        aggrTime.set(NANOSECONDS.toMillis(eval_elapse))
        elapseTime.set(NANOSECONDS.toMillis(elapse))
        logInfo(s"HashAggregate Completed, total processed ${numInputBatches.value} batches, took ${NANOSECONDS.toMillis(elapse)} ms handling one file(including fetching + processing), took ${NANOSECONDS.toMillis(eval_elapse)} ms doing evaluation.");
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
      aggrTime: SQLMetric,
      elapseTime: SQLMetric): ColumnarAggregation = synchronized {
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
      aggrTime,
      elapseTime)
    columnarAggregation
  }

  def close(): Unit = {
    if (columnarAggregation != null) {
      columnarAggregation.close()
      columnarAggregation = null
    }
  }
}
