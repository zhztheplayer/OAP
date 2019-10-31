package com.intel.sparkColumnarPlugin.expression

import io.netty.buffer.ArrowBuf
import java.util.ArrayList
import java.util.concurrent.TimeUnit

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

  logInfo(
    s"groupingExpressions: $groupingExpressions,\noriginalInputAttributes: $originalInputAttributes,\naggregateExpressions: $aggregateExpressions,\naggregateAttributes: $aggregateAttributes,\nresultExpressions: $resultExpressions")

  val aggregateColumnarExpressions: Seq[Expression] =
    aggregateExpressions.map(expr =>
      ColumnarExpressionConverter.replaceWithColumnarExpression(expr))

  val resultSchema = StructType(
    aggregateAttributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
  var resultTotalRows: Int = 0
  val resultCache = new ArrayList[ColumnarBatch]()
  var aggregator: ExpressionEvaluator = _
  var finalAggregator: ExpressionEvaluator = _
  var resultArrowSchema: Schema = _

  def createAggregation(input: ColumnarBatch) {
    val fieldTypesList = List
      .range(0, input.numCols())
      .map(i => Field.nullable(s"c_$i", CodeGeneration.getResultType(input.column(i).dataType())))

    var col_id = 0
    val gandivaExpressionTree: Seq[(ExpressionTree, Field, ExpressionTree)] =
      (aggregateColumnarExpressions zip aggregateAttributes)
        .map {
          case (expr, attr) => {
            val (node, result, finalNode) =
              expr
                .asInstanceOf[ColumnarAggregateExpression]
                .doColumnarCodeGen_ext((fieldTypesList(col_id), attr, s"result_${col_id}"))
            col_id += 1
            if (node == null) {
              null
            } else {
              (
                TreeBuilder.makeExpression(node, result),
                result,
                TreeBuilder.makeExpression(finalNode, result))
            }
          }
        }
        .filter(_ != null)

    val arrowSchema = new Schema(fieldTypesList.asJava)
    resultArrowSchema = new Schema(gandivaExpressionTree.map(_._2).toList.asJava)

    aggregator = new ExpressionEvaluator()
    aggregator.build(arrowSchema, gandivaExpressionTree.map(_._1).toList.asJava)

    finalAggregator = new ExpressionEvaluator()
    finalAggregator.build(resultArrowSchema, gandivaExpressionTree.map(_._3).toList.asJava)
  }

  def close(): Unit = {
    aggregator.close()
  }

  def updateAggregationResult(columnarBatch: ColumnarBatch): Unit = {
    if (aggregator == null) {
      createAggregation(columnarBatch)
    }
    val inputRecordBatch = createArrowRecordBatch(columnarBatch)
    val resultRecordBatchList = aggregator.evaluate(inputRecordBatch)
    val resultRecordBatch = resultRecordBatchList(0)
    releaseArrowRecordBatch(inputRecordBatch)
    val resultColumnarBatch = fromArrowRecordBatch(resultArrowSchema, resultRecordBatch)
    releaseArrowRecordBatch(resultRecordBatch)

    resultCache.add(resultColumnarBatch);
    resultTotalRows += resultColumnarBatch.numRows()
    //logInfo(s"getAggregationResult one batch done, result is ${resultColumnarBatch.column(0).getUTF8String(0)}")
  }

  def getAggregationResult(): ColumnarBatch = {
    val resultColumnVectors =
      ArrowWritableColumnVector.allocateColumns(resultTotalRows, resultSchema).toArray
    var rowId: Int = 0;
    resultCache.asScala.map(columnarBatch => {
      // get rows from cached columnarBatch and put into the final one
      for (i <- 0 until columnarBatch.numCols()) {
        columnarBatch
          .column(i)
          .asInstanceOf[ArrowWritableColumnVector]
          .mergeTo(resultColumnVectors(i), rowId)
      }
      rowId += columnarBatch.numRows()
      columnarBatch.close()
    })
    val finalInputColumnarBatch =
      new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), rowId)
    val finalInputRecordBatch = createArrowRecordBatch(finalInputColumnarBatch)
    val finalResultRecordBatchList = finalAggregator.evaluate(finalInputRecordBatch)
    val finalResultRecordBatch = finalResultRecordBatchList(0)
    releaseArrowRecordBatch(finalInputRecordBatch)
    finalInputColumnarBatch.close()

    val finalColumnarBatch = fromArrowRecordBatch(resultArrowSchema, finalResultRecordBatch)
    releaseArrowRecordBatch(finalResultRecordBatch)

    //logInfo(s"getAggregationResult done, result is ${finalColumnarBatch.column(0).getUTF8String(0)}")
    finalColumnarBatch
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
          updateAggregationResult(cb)
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
    recordBatch.close();
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
