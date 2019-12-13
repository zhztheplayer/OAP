package com.intel.sparkColumnarPlugin.expression

import java.util.concurrent.TimeUnit._
import com.google.common.collect.Lists

import com.intel.sparkColumnarPlugin.vectorized.ExpressionEvaluator

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.vectorized.ArrowWritableColumnVector
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.types._
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

class ColumnarSorter(
    sortOrder: Seq[SortOrder],
    outputAttributes: Seq[Attribute],
    sortTime: SQLMetric)
    extends Logging {

  logInfo(s"ColumnarSorter sortOrder is ${sortOrder}, outputAttributes is ${outputAttributes}")
  if (sortOrder.length != 1) {
    throw new UnsupportedOperationException("ColumnarSorter does not support multiple key sort yet.")
  }
  /////////////// Prepare ColumnarSorter //////////////
  var numInputBatches: Long = 0
  var processedNumRows: Long = 0
  val inputBatchHolder = new ListBuffer[ArrowRecordBatch]() 
  val keyField: Field = {
    val attr = ConverterUtils.getAttrFromExpr(sortOrder.head.child)
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  }
  val outputFieldList: List[Field] = outputAttributes.toList.map(expr => {
    val attr = ConverterUtils.getAttrFromExpr(expr)
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  })
  val inputFieldList: List[Field] = outputFieldList

  val gandivaExpressionTree: List[ExpressionTree] = inputFieldList.map( inputField => {
    val funcName = "action_dono"
    val resultType = inputField.getType()
    val sortNode = TreeBuilder.makeFunction(
        "sortArraysToIndices",
        Lists.newArrayList(TreeBuilder.makeField(keyField)),
        keyField.getType())
    val shuffleNode = TreeBuilder.makeFunction(
        "shuffleArrayList",
        (sortNode :: inputFieldList.map({field => TreeBuilder.makeField(field)})).asJava,
        resultType)
    val funcNode = TreeBuilder.makeFunction(
        funcName,
        Lists.newArrayList(shuffleNode, TreeBuilder.makeField(inputField)),
        resultType)

    TreeBuilder.makeExpression(funcNode, inputField)
  })

  var sorter = new ExpressionEvaluator()
  val arrowSchema = new Schema(inputFieldList.asJava)

  logInfo(s"inputFieldList is ${inputFieldList}, outputFieldList is ${outputFieldList}")
  sorter.build(arrowSchema, gandivaExpressionTree.asJava, true/*return at finish*/)

  /////////////////////////////////////////////////////
  
  def close(): Unit = {
    logInfo("ColumnarSorter ExpressionEvaluator closed");
    sorter.close()
    sorter = null
  }

  def updateSorterResult(input: ColumnarBatch): Unit = {
    val input_batch = ConverterUtils.createArrowRecordBatch(input)
    inputBatchHolder += input_batch
    sorter.evaluate(input_batch)
  }

  def getSorterResult(): ColumnarBatch = {
    val resultSchema = StructType(outputAttributes.map(expr => {
      val attr = ConverterUtils.getAttrFromExpr(expr)
      StructField(s"${attr.name}", attr.dataType, true)
    }).toArray)

    val res = if (processedNumRows == 0) {
      val resultColumnVectors =
        ArrowWritableColumnVector.allocateColumns(0, resultSchema).toArray
      new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
    } else {
      val finalResultRecordBatchList = sorter.finish()
      if (finalResultRecordBatchList.size == 0) {
        val resultColumnVectors =
          ArrowWritableColumnVector.allocateColumns(0, resultSchema).toArray
        new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
      } else {
        val recordBatchSchema = new Schema(outputFieldList.asJava)
        val resultColumnVectorList = ConverterUtils.fromArrowRecordBatch(
          recordBatchSchema, finalResultRecordBatchList(0))

        val finalColumnarBatch = new ColumnarBatch(resultColumnVectorList.map(v => v.asInstanceOf[ColumnVector]), finalResultRecordBatchList(0).getLength())
        ConverterUtils.releaseArrowRecordBatchList(finalResultRecordBatchList)
        finalColumnarBatch
      }
    }
    ConverterUtils.releaseArrowRecordBatchList(inputBatchHolder.toArray)
    res
  }

  def createIterator(cbIterator: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      var cb: ColumnarBatch = null

      override def hasNext: Boolean = {
        cbIterator.hasNext
      }

      override def next(): ColumnarBatch = {
        val beforeSort = System.nanoTime()
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
            updateSorterResult(cb)
            processedNumRows += cb.numRows
            numInputBatches += 1
          }
          eval_elapse += System.nanoTime() - beforeEval
        }
        val beforeFinish = System.nanoTime()
        val outputBatch = getSorterResult()
        finish_elapse += System.nanoTime() - beforeFinish
        val elapse = System.nanoTime() - beforeSort
        sortTime.set(NANOSECONDS.toMillis(elapse))
        logInfo(s"Sort Completed, total processed ${numInputBatches} batches, took ${NANOSECONDS.toMillis(elapse)} ms handling one file(including fetching + processing), took ${NANOSECONDS.toMillis(eval_elapse)} ms doing evaluation, ${NANOSECONDS.toMillis(finish_elapse)} ms doing finish process.");
        if (cb != null) {
          cb.close()
          cb = null
        }
        outputBatch
      }
    }
  }

}

object ColumnarSorter {
  var columnarSorter: ColumnarSorter = _
  def create(
      sortOrder: Seq[SortOrder],
      outputAttributes: Seq[Attribute],
      sortTime: SQLMetric): ColumnarSorter = synchronized {
    columnarSorter = new ColumnarSorter(
      sortOrder,
      outputAttributes,
      sortTime)
    columnarSorter
  }

  def close(): Unit = {
    if (columnarSorter != null) {
      columnarSorter.close()
      columnarSorter = null
    }
  }
}
