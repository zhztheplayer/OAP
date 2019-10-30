package com.intel.sparkColumnarPlugin.expression

import io.netty.buffer.ArrowBuf
import java.util._
import java.util.concurrent.TimeUnit

import com.google.common.collect.Lists
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.sql.execution.vectorized.ArrowWritableColumnVector

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.ArrowType

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.List
import scala.collection.mutable.ArrayBuffer

class ColumnarProjection(exprs: Seq[Expression])
    extends (ColumnarBatch => ColumnarBatch)
    with AutoCloseable
    with Logging {
  // build gandiva projection here.
  var elapseTime_make: Long = 0
  ColumnarExpressionConverter.reset()
  val columnarExprs: Seq[Expression] = exprs.map(expr => {
    ColumnarExpressionConverter.replaceWithColumnarExpression(expr)
  })
  val check_if_only_BoundReference = ColumnarExpressionConverter.ifNoCalculation
  var schema: StructType = _
  var projector: Projector = _

  def initialize(): Unit = {}

  override def close(): Unit = {
    if (projector != null) {
      projector.close()
    }
  }

  def apply(columnarBatch: ColumnarBatch): ColumnarBatch = {
    if (check_if_only_BoundReference) {
      columnarBatch.retain()
      return columnarBatch
    }
    if (schema == null || projector == null) {
      val start_make: Long = System.nanoTime()
      val fieldTypesList = List
        .range(0, columnarBatch.numCols())
        .map(i =>
          Field
            .nullable(s"c_$i", CodeGeneration.getResultType(columnarBatch.column(i).dataType())))
      val prepareList: Seq[(ExpressionTree, ArrowType)] = columnarExprs
        .map(expr => {
          val (node, resultType) =
            expr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(fieldTypesList)
          if (node == null) {
            null
          } else {
            val result = Field.nullable("result", resultType)
            (TreeBuilder.makeExpression(node, result), resultType)
          }
        })
        .filter(_ != null)
      val resultSchema = new Schema(
        prepareList.map(expr => Field.nullable(s"result", expr._2)).toList.asJava)
      val arrowSchema = new Schema(fieldTypesList.asJava)
      logInfo(s"arrowSchema is $arrowSchema")

      schema = createStructType(resultSchema)
      projector = createProjector(arrowSchema, prepareList)
      elapseTime_make = System.nanoTime() - start_make
      logInfo(s"Gandiva make total ${TimeUnit.NANOSECONDS.toMillis(elapseTime_make)} ms.")
    }

    val resultColumnVectors =
      ArrowWritableColumnVector.allocateColumns(columnarBatch.numRows(), schema).toArray
    val eval_start = System.nanoTime()
    if (columnarBatch.numRows() != 0) {
      val input = createArrowRecordBatch(columnarBatch)
      val outputVectors =
        resultColumnVectors.map(columnVector => columnVector.getValueVector()).toList.asJava
      projector.evaluate(input, outputVectors)
      releaseArrowRecordBatch(input)

    }
    val metrics = columnarBatch.taskId().asInstanceOf[Array[Long]];
    metrics(2) += System.nanoTime() - eval_start

    val resultColumnarBatch = new ColumnarBatch(
      resultColumnVectors.map(_.asInstanceOf[ColumnVector]),
      columnarBatch.numRows())
    resultColumnarBatch
  }

  def createProjector(
      arrowSchema: Schema,
      prepareList: Seq[(ExpressionTree, ArrowType)]): Projector = synchronized {
    if (projector != null) {
      return projector
    }
    val fieldNodesList = prepareList.map(_._1).toList.asJava
    logInfo(s"fieldNodesList is $fieldNodesList")
    Projector.make(arrowSchema, fieldNodesList)
  }

  def createStructType(arrowSchema: Schema): StructType = synchronized {
    if (schema != null) {
      return schema
    }
    ArrowUtils.fromArrowSchema(arrowSchema)
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

  def releaseArrowRecordBatch(recordBatch: ArrowRecordBatch): Unit = {
    recordBatch.close();
  }
}

object ColumnarProjection
    extends CodeGeneratorWithInterpretedFallback[Seq[Expression], ColumnarProjection]
    with AutoCloseable {

  var columnarProjection: ColumnarProjection = _
  override protected def createCodeGeneratedObject(in: Seq[Expression]): ColumnarProjection = {
    throw new UnsupportedOperationException(
      s"${this.getClass} createCodeGeneratedObject is not currently supported.")
  }

  override protected def createInterpretedObject(in: Seq[Expression]): ColumnarProjection = {
    throw new UnsupportedOperationException(
      s"${this.getClass} createInterpretedObject is not currently supported.")
  }

  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): ColumnarProjection =
    synchronized {
      // make gandiva projection here.
      //if (columnarProjection == null) {
      columnarProjection = new ColumnarProjection(bindReferences(exprs, inputSchema))
      //}
      columnarProjection
    }

  override def close(): Unit = {
    if (columnarProjection != null) {
      columnarProjection.close()
    }
  }
}
