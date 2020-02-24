package com.intel.sparkColumnarPlugin.expression

import io.netty.buffer.ArrowBuf
import java.util.ArrayList
import java.util.Collections
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
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.ArrowType

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.List
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

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

  logInfo(
    s"\ngroupingExpressions: $groupingExpressions,\noriginalInputAttributes: $originalInputAttributes,\naggregateExpressions: $aggregateExpressions,\naggregateAttributes: $aggregateAttributes,\nresultExpressions: $resultExpressions")

  var resultTotalRows: Int = 0
  val inputAttributeSeq: AttributeSeq = originalInputAttributes
  val mode = aggregateExpressions(0).mode
  val keyOrdinalList: List[Int] = groupingExpressions.toList.map(expr => {
    val attr = ConverterUtils.getAttrFromExpr(expr)
    val bindReference = BindReferences.bindReference(attr.asInstanceOf[Expression], originalInputAttributes)
    getOrdinalFromExpr(bindReference)
  })
  val keyFieldList: List[Field] = keyOrdinalList.map(i => {
    val attr = ConverterUtils.getAttrFromExpr(originalInputAttributes(i))
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })
  //////////////// Project original input to aggregate input //////////////////
  var projector : Projector = _
  var projOrdinalList : List[Int] = _
  var aggrFieldList : List[Field] = _
  var projInputList : java.util.List[Field] = _
  var projArrowSchema: Schema = _
  mode match {
    case Partial => { 
      projInputList = Lists.newArrayList()
      val projPrepareList : Seq[(ExpressionTree, Field)] =
        aggregateExpressions.flatMap(_.aggregateFunction.children).zipWithIndex
          .filter{case (expr, i) => !expr.isInstanceOf[Literal]}
          .map{
          case (expr, i) => {
            val columnarExpr: Expression =
              ColumnarExpressionConverter.replaceWithColumnarExpression(expr, originalInputAttributes)
            logInfo(s"columnarExpr is ${columnarExpr}")
            val (node, resultType) =
              columnarExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(projInputList)
            val result = Field.nullable(s"res_$i", resultType)
            (TreeBuilder.makeExpression(node, result), result)
          }
        }
      Collections.sort(projInputList, (l: Field, r: Field) => { l.getName.compareTo(r.getName)})
      projOrdinalList = projInputList.asScala.toList.map(field => {
        field.getName.replace("c_", "").toInt
      })
      aggrFieldList = projPrepareList.map(_._2).toList
    
      projArrowSchema = new Schema(projInputList) 
      if (projPrepareList.length > 0) {
        projector = Projector.make(projArrowSchema, projPrepareList.map(_._1).toList.asJava)
      }
    }
    case Final => {
      val ordinal_field_list : List[(Int, Field)] = originalInputAttributes.toList.zipWithIndex
        .filter{case(expr, i) => !keyOrdinalList.contains(i)}
        .map{case(expr, i) => {
          val attr = ConverterUtils.getAttrFromExpr(expr)
          (i, Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType)))
        }}
      projOrdinalList = ordinal_field_list.map(_._1)
      aggrFieldList = ordinal_field_list.map(_._2)
      projInputList = aggrFieldList.asJava
      projArrowSchema = new Schema(projInputList)
    }
    case _ =>
      throw new UnsupportedOperationException("doesn't support this mode")
  }
  logInfo(s"Project input ordinal is ${projOrdinalList}, Schema is ${projArrowSchema}")
  /////////////////////////////////////////////////////////////////////////////

  val inputFieldList = keyFieldList ::: aggrFieldList

  val outputFieldList: List[Field] = resultExpressions.toList.map( expr => {
    val attr = ConverterUtils.getAttrFromExpr(expr)
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })

  // map expression
  var field_id = 0
  var aggr_id = 0
  val expressionsBuffer = ListBuffer[ColumnarAggregateExpressionBase]()
  var i = 0
  val groupExpressionCached = Map[Int, List[Int]]()
  val aggrExpressionCached = Map[Int, List[Int]]()
  val expressionToResult = ListBuffer[Int]()
  val expressionResultFieldList = ListBuffer[Field]()
  while (i < resultExpressions.length) {
    val expr = resultExpressions(i)
    val attr = ConverterUtils.getAttrFromExpr(expr, skipAlias = true)
    var ordinal = groupingExpressions.indexWhere(_.exprId == attr.exprId) 
    if (ordinal != -1) {
      if (!groupExpressionCached.contains(ordinal)) {
        groupExpressionCached += ordinal -> List(i)
        val a = groupingExpressions(ordinal)
        logInfo(s"expr is $attr, groupingExpression is ${ordinal}: $a")
        val field = Field.nullable(s"${a.name}#${a.exprId.id}", CodeGeneration.getResultType(a.dataType))
        val resField = Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
        expressionResultFieldList += resField
        expressionsBuffer +=
          new ColumnarUniqueAggregateExpression(List(field)).asInstanceOf[ColumnarAggregateExpressionBase]
      }
      groupExpressionCached(ordinal).foreach{j =>
        expressionToResult += j
      }
      i += 1
    } else {
      ordinal = AttributeSeq(aggregateAttributes).indexOf(attr.exprId)
      if (ordinal == -1) {
        ordinal = aggr_id
      }
      if (!aggrExpressionCached.contains(ordinal)) {
        val a = aggregateExpressions(ordinal)
        logInfo(s"expr is $attr, aggrExpression is ${ordinal}: $a")
        val res = new ColumnarAggregateExpression(
          a.aggregateFunction,
          a.mode,
          a.isDistinct,
          a.resultId)
        val arg_size = res.requiredColNum
        val res_size = res.expectedResColNum
        val tmp_list: List[Int] = i until i + res_size toList
        val fieldList = ListBuffer[Field]()
        for (j <- 0 until arg_size) {
          fieldList += aggrFieldList(field_id)
          field_id += 1
        }
        res.setInputFields(fieldList.toList)
        aggrExpressionCached += ordinal -> tmp_list
        tmp_list.foreach(j => {
          val resAttr = ConverterUtils.getAttrFromExpr(resultExpressions(j), true)
          val resField = Field.nullable(s"${resAttr.name}#${resAttr.exprId.id}", CodeGeneration.getResultType(resAttr.dataType))
          expressionResultFieldList += resField
        })
        aggr_id += 1
        expressionsBuffer +=
          res.asInstanceOf[ColumnarAggregateExpressionBase]
      }
      aggrExpressionCached(ordinal).foreach{j =>
        expressionToResult += j
      }
      i += aggrExpressionCached(ordinal).size
    }
  } 
  val expressions = expressionsBuffer.toList

  logInfo(s"inputFieldList is ${inputFieldList}, expressionResultFieldList is ${expressionResultFieldList}, outputFieldList is ${outputFieldList}, expressionToResult is ${expressionToResult}")

  /* declare dummy resultType and resultField to generate Gandiva expression
   * Both won't be actually used.*/
  val resultType = CodeGeneration.getResultType()
  val resultField = Field.nullable(s"dummy_res", resultType)

  val gandivaExpressionTree: List[(ExpressionTree, ExpressionTree)] = expressions.map( expr => {
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
  })

  val aggrExprResultSchema = new Schema(expressionResultFieldList.toList.asJava)
  val resultSchema = new Schema(outputFieldList.asJava)
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

  aggregator.setReturnFields(aggrExprResultSchema)

  def ifIn(name: String, attrList: List[Field]): Boolean = {
    var found = false
    attrList.foreach(attr => {
      if (attr.getName == name) {
        found = true
      }
    })
    found
  }

  def getOrdinalFromExpr(expr: Expression): Int = {
    expr match {
      case e: BoundReference =>
        e.ordinal
      case other =>
        throw new UnsupportedOperationException(s"getOrdinalFromExpr is unable to parse from $other (${other.getClass}).")
    }
  }

  def close(): Unit = {
    logInfo(" closed");
    if (aggregator != null) {
      aggregator.close()
      aggregator = null
    }
    if (projector != null) {
      projector.close()
      projector = null
    }
  }

  def updateAggregationResult(columnarBatch: ColumnarBatch): Unit = {
    val numRows = columnarBatch.numRows
    val projCols = projOrdinalList.map(i => {
      columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
    })
    val aggrCols : Array[ValueVector] = if (projector != null) {
      val inputProjRecordBatch: ArrowRecordBatch = ConverterUtils.createArrowRecordBatch(numRows, projCols)
      val aggrArrowSchema = new Schema(aggrFieldList.asJava)
      val aggrSchema = ArrowUtils.fromArrowSchema(aggrArrowSchema)
      val outputVectors = ArrowWritableColumnVector.allocateColumns(numRows, aggrSchema)
        .map(columnVector => columnVector.getValueVector())
      projector.evaluate(inputProjRecordBatch, outputVectors.toList.asJava)
      ConverterUtils.releaseArrowRecordBatch(inputProjRecordBatch)
      outputVectors.toArray
    } else {
      projCols.toArray
    }
    val groupCols = keyOrdinalList.map(i => {
      columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
    })
    val inputAggrRecordBatch: ArrowRecordBatch = ConverterUtils.createArrowRecordBatch(numRows, groupCols ++ aggrCols)
    aggregator.evaluate(inputAggrRecordBatch)
   
    projCols.map(v => v.close())
    aggrCols.map(v => v.close())
    groupCols.map(v => v.close())
    ConverterUtils.releaseArrowRecordBatch(inputAggrRecordBatch)
  }

  def getAggregationResult(): ColumnarBatch = {
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
      val finalResultRecordBatch = finalResultRecordBatchList(0)
      logInfo(s"finalResultRecordBatch has numRows ${finalResultRecordBatch.getLength}, fields as ${finalResultRecordBatch.getNodes}, expected schema as ${resultSchema}")
      val aggrExprResultColumnVectorList = ConverterUtils.fromArrowRecordBatch(aggrExprResultSchema, finalResultRecordBatch)
      // map from aggr result to final result
      val resultColumnVectorList = expressionToResult.toList.map(i => aggrExprResultColumnVectorList(i)).toArray

      val finalColumnarBatch = new ColumnarBatch(resultColumnVectorList.map(v => v.asInstanceOf[ColumnVector]), finalResultRecordBatchList(0).getLength())
      ConverterUtils.releaseArrowRecordBatchList(finalResultRecordBatchList)
      finalColumnarBatch
    }
  }

  def createIterator(cbIterator: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      var cb: ColumnarBatch = null

      override def hasNext: Boolean = cbIterator.hasNext

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
