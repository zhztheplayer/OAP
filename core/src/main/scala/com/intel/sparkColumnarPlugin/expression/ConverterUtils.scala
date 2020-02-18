package com.intel.sparkColumnarPlugin.expression

import io.netty.buffer.ArrowBuf

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.vectorized.ArrowWritableColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object ConverterUtils extends Logging {
  def createArrowRecordBatch(columnarBatch: ColumnarBatch): ArrowRecordBatch = {
    val fieldNodes = new ListBuffer[ArrowFieldNode]()
    val inputData = new ListBuffer[ArrowBuf]()
    val numRowsInBatch = columnarBatch.numRows()
    for (i <- 0 until columnarBatch.numCols()) {
      val inputVector =
        columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
      fieldNodes += new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount())
      inputData += inputVector.getValidityBuffer()
      if (inputVector.isInstanceOf[VarCharVector]) {
        inputData += inputVector.getOffsetBuffer()
      }
      inputData += inputVector.getDataBuffer()
    }
    new ArrowRecordBatch(numRowsInBatch, fieldNodes.toList.asJava, inputData.toList.asJava)
  }

  def createArrowRecordBatch(numRowsInBatch: Int, cols: List[ValueVector]): ArrowRecordBatch = {
    val fieldNodes = new ListBuffer[ArrowFieldNode]()
    val inputData = new ListBuffer[ArrowBuf]()
    cols.foreach(inputVector => {
      fieldNodes += new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount())
      inputData += inputVector.getValidityBuffer()
      if (inputVector.isInstanceOf[VarCharVector]) {
        inputData += inputVector.getOffsetBuffer()
      }
      inputData += inputVector.getDataBuffer()
    })
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
        releaseArrowRecordBatch(recordBatch)
    })
  }

  def getAttrFromExpr(fieldExpr: Expression): AttributeReference = {
    fieldExpr match {
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

  def combineArrowRecordBatch(rb1: ArrowRecordBatch, rb2: ArrowRecordBatch): ArrowRecordBatch = {
     val numRows = rb1.getLength()
     val rb1_nodes = rb1.getNodes()
     val rb2_nodes = rb2.getNodes()
     val rb1_bufferlist = rb1.getBuffers()
     val rb2_bufferlist = rb2.getBuffers()

     val combined_nodes = rb1_nodes.addAll(rb2_nodes)
     val combined_bufferlist = rb1_bufferlist.addAll(rb2_bufferlist)
     new ArrowRecordBatch(numRows, rb1_nodes, rb1_bufferlist)
  }

}

