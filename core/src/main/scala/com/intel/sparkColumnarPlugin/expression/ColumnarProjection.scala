package com.intel.sparkColumnarPlugin.expression

import io.netty.buffer.ArrowBuf
import java.util._

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

class ColumnarProjection(exprs: Seq[Expression]) extends (ColumnarBatch => ColumnarBatch) with Logging {
  // build gandiva projection here.
  val columnarExprs: Seq[Expression] = exprs.map(
    expr => {
      ColumnarExpressionConverter.replaceWithColumnarExpression(expr)
    }
  )
  val projectorList: Seq[(Projector, ArrowType)] = columnarExprs.map(
    expr => {
      val fieldTypes: ListBuffer[Field] = new ListBuffer[Field]();
      val (node, resultType): (TreeNode, ArrowType) = expr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(fieldTypes)
      val result: Field = Field.nullable("result", resultType);
      (makeProjector(node, result, fieldTypes), resultType)
    }
  )

  def initialize(): Unit = {

  }

  def apply(columnarBatch: ColumnarBatch): ColumnarBatch = {
    // call gandiva evaluate to process
    // and return result as ColumnarBatch
    //logInfo("apply columnarBatch")
    val resultColumnVectors = projectorList.map {
      case (projector, resultType) => {
        val input = createArrowRecordBatch(columnarBatch)
        val output = createArrowColumnVectors(columnarBatch.numRows(), resultType)
        val outputVectors = output.map(
          columnVector => {
            //logInfo(s"createArrowColumnVectors for result, numRows: ${columnarBatch.numRows()}, columnVector: $columnVector")
            columnVector.asInstanceOf[ArrowWritableColumnVector].getValueVector()
          }
        ).toList
        //logInfo(s"start to evaluate")
        projector.evaluate(input, outputVectors.asJava)
        releaseArrowRecordBatch(input)
        output(0)
      }
    }.toArray
    new ColumnarBatch(resultColumnVectors, columnarBatch.numRows())  
  }

  def makeProjector(node: TreeNode, result: Field, inputDataTypes: ListBuffer[Field]): Projector = {
    val exprTree = TreeBuilder.makeExpression(node, result) :: Nil
    val schema = new Schema(inputDataTypes.toList.asJava)
    Projector.make(schema, exprTree.asJava)
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

  def createArrowColumnVectors(numRowInBatch: Int, resultType: ArrowType): Array[ColumnVector] = {
    val schema = StructType(StructField("result", ArrowUtils.fromArrowType(resultType), true) :: Nil)
    ArrowWritableColumnVector.allocateColumns(numRowInBatch, schema).toArray
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
