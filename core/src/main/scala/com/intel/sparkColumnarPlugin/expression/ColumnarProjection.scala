package com.intel.sparkColumnarPlugin.expression

import io.netty.buffer.ArrowBuf
import java.util._
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists
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
import scala.collection.mutable.ArrayBuffer

class ColumnarProjection(exprs: Seq[Expression])
  extends (ColumnarBatch => ColumnarBatch) with AutoCloseable with Logging {
  // build gandiva projection here.
  var elapseTime_make: Long = 0
  var elapseTime_process: Long = 0
  var process_last_end: Long = 0
  var elapseTime_eval: Long = 0
  val make_start = System.nanoTime()
  val columnarExprs: Seq[Expression] = exprs.map(ColumnarExpressionConverter.replaceWithColumnarExpression(_))
  val prepareList: Seq[(ExpressionTree, ListBuffer[Field], ArrowType)] = columnarExprs.map(
    expr => {
      val fieldTypes = new ListBuffer[Field]()
      val (node, resultType) = expr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(fieldTypes)
      val result = Field.nullable("result", resultType)
      (TreeBuilder.makeExpression(node, result), fieldTypes, resultType)
    })
  val projector = Projector.make(new Schema(prepareList(0)._2.toList.asJava),
                                 prepareList.map(_._1).toList.asJava)
  val arrowSchema = new Schema(prepareList.map(args => Field.nullable("result", args._3)).toList.asJava)
  val schema = ArrowUtils.fromArrowSchema(arrowSchema)
  elapseTime_make += System.nanoTime() - make_start

  def initialize(): Unit = {
    process_last_end = System.nanoTime()

  }

  override def close(): Unit = {
    logInfo(s"Gandiva Make took ${TimeUnit.NANOSECONDS.toMillis(elapseTime_make)} ms.")
    logInfo(s"Gandiva Process took total ${TimeUnit.NANOSECONDS.toMillis(elapseTime_eval)} ms.")
    logInfo(s"End to end took total ${TimeUnit.NANOSECONDS.toMillis(elapseTime_process)} ms.")
  }

  def apply(columnarBatch: ColumnarBatch): ColumnarBatch = {
    // call gandiva evaluate to process
    // and return result as ColumnarBatch
    //logInfo("apply columnarBatch")
    val eval_start = System.nanoTime()
    val input = createArrowRecordBatch(columnarBatch)
    val resultColumnVectors = ArrowWritableColumnVector.allocateColumns(
      columnarBatch.numRows(), schema).toArray
    val outputVectors = resultColumnVectors.map(
      columnVector => columnVector.getValueVector()
    ).toList
    projector.evaluate(input, outputVectors.asJava)
    releaseArrowRecordBatch(input)

    elapseTime_eval += System.nanoTime() - eval_start
    elapseTime_process += System.nanoTime() - process_last_end
    process_last_end = System.nanoTime()

    new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), columnarBatch.numRows())  
  }

  def createArrowRecordBatch(columnarBatch: ColumnarBatch): ArrowRecordBatch = {
    val fieldNodes = new ListBuffer[ArrowFieldNode]()
    val inputData = new ListBuffer[ArrowBuf]()
    val numRowsInBatch = columnarBatch.numRows()
    for (i <- 0 until columnarBatch.numCols()) {
      //logInfo(s"createArrowRecordBatch from columnVector: ${columnarBatch.column(i)}")
      val inputVector = columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
      fieldNodes += new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount())
      inputData += inputVector.getValidityBuffer()
      inputData += inputVector.getDataBuffer()
    }
    new ArrowRecordBatch(numRowsInBatch, fieldNodes.toList.asJava, inputData.toList.asJava)
  }

 def releaseArrowRecordBatch(recordBatch: ArrowRecordBatch): Unit = {
    val buffers = recordBatch.getBuffers();
    recordBatch.close();
  }
}

object ColumnarProjection
    extends CodeGeneratorWithInterpretedFallback[Seq[Expression], ColumnarProjection] {

  override protected def createCodeGeneratedObject(in: Seq[Expression]): ColumnarProjection = {
    throw new UnsupportedOperationException(
      s"${this.getClass} createCodeGeneratedObject is not currently supported.")
  }

  override protected def createInterpretedObject(in: Seq[Expression]): ColumnarProjection = {
    throw new UnsupportedOperationException(
      s"${this.getClass} createInterpretedObject is not currently supported.")
  }

  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): ColumnarProjection = {
    // make gandiva projection here.
    new ColumnarProjection(bindReferences(exprs, inputSchema))
  }
}
