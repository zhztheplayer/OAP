package com.intel.sparkColumnarPlugin.expression

import io.netty.buffer.ArrowBuf
import java.lang._
import java.util._
import java.util.concurrent.TimeUnit

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
import org.apache.arrow.gandiva.ipc.GandivaTypes.SelectionVectorType
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.ArrowType

import scala.collection.JavaConverters._
import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

class ColumnarFilter(expr: Expression)
  extends (ColumnarBatch => ColumnarBatch) with AutoCloseable with Logging {
  // build gandiva projection here.
  var elapseTime_make: Long = 0
  var elapseTime_eval: Long = 0
  val columnarExpr: Expression = ColumnarExpressionConverter.replaceWithColumnarExpression(expr)
  

  var filter: Filter = _
  var projector: Projector = _
  var schema: StructType = _

  val allocator = new RootAllocator(Long.MAX_VALUE)
  var selectionBuffer: ArrowBuf = _

  def initialize(): Unit = {

  }
  
  override def close(): Unit = {
    selectionBuffer.close()
    allocator.close()
    filter.close()
    projector.close()
  }

  def apply(columnarBatch: ColumnarBatch): ColumnarBatch = {
    //logInfo(s"apply ${columnarBatch}")
    if (schema == null || filter == null || projector == null) {
      val start_make: Long = System.nanoTime()
      val fieldTypesList = List.range(0, columnarBatch.numCols()).map(i =>
        Field.nullable(s"c_$i", CodeGeneration.getResultType(columnarBatch.column(i).dataType()))
      )
      val arrowSchema = new Schema(fieldTypesList.asJava)

      val prepareList: (TreeNode, ArrowType) = {
        val (cond, resultType) =
          columnarExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(fieldTypesList)
        (cond, resultType)
      }

      schema = createStructType(arrowSchema)
      filter = createFilter(arrowSchema, prepareList)
      val exprTreeList = List.range(0, columnarBatch.numCols()).map(i => {
        val dataType = CodeGeneration.getResultType(columnarBatch.column(i).dataType())
        TreeBuilder.makeExpression(TreeBuilder.makeField(Field.nullable(s"c_$i", dataType)),
                                   Field.nullable("result", dataType))
      }).asJava
      projector = createProjector(arrowSchema, exprTreeList)
      elapseTime_make = System.nanoTime() - start_make
      logInfo(s"Gandiva make total ${TimeUnit.NANOSECONDS.toMillis(elapseTime_make)} ms.")
    }

    if (selectionBuffer == null) {
      selectionBuffer = allocator.buffer(columnarBatch.numRows() * 2)
    }

    if (columnarBatch.numRows() != 0) {
      val eval_start = System.nanoTime()
      val input = createArrowRecordBatch(columnarBatch)
      val selectionVector = new SelectionVectorInt16(selectionBuffer)
      filter.evaluate(input, selectionVector)

      // TODO: apply selectedVector onto columnarBatch
      val resultColumnVectors = ArrowWritableColumnVector.allocateColumns(
        selectionVector.getRecordCount(), schema).toArray
      val outputVectors = resultColumnVectors.map(
        columnVector => columnVector.getValueVector()
      ).toList.asJava
      projector.evaluate(input, selectionVector, outputVectors);

      releaseArrowRecordBatch(input)
      elapseTime_eval += System.nanoTime() - eval_start

      logInfo(s"Gandiva filter evaluated, result numLines is ${selectionVector.getRecordCount()}.")
      new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), selectionVector.getRecordCount())
    } else {
      val resultColumnVectors = ArrowWritableColumnVector.allocateColumns(
        columnarBatch.numRows(), schema).toArray
      logInfo(s"Gandiva filter evaluated, result numLines is ${columnarBatch.numRows()}.")
      new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), columnarBatch.numRows())
    }
  }

  def createFilter(arrowSchema: Schema, prepareList: (TreeNode, ArrowType)): Filter = synchronized {
    if (filter != null) {
      return filter
    }
    Filter.make(arrowSchema, TreeBuilder.makeCondition(prepareList._1))
  }

  def createProjector(arrowSchema: Schema, exprTreeList: java.util.List[ExpressionTree]): Projector = synchronized {
    if (projector != null) {
      return projector
    }
    Projector.make(arrowSchema, exprTreeList, SelectionVectorType.SV_INT16)
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
      //logInfo(s"createArrowRecordBatch from columnVector: ${columnarBatch.column(i)}")
      val inputVector = columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
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

object ColumnarFilter extends AutoCloseable {

  var columnarFilter:ColumnarFilter = _

  def create(condition: Expression, inputSchema: Seq[Attribute]): ColumnarFilter = synchronized  {
    // make gandiva projection here.
    if (columnarFilter == null) {
      columnarFilter = new ColumnarFilter(BindReferences.bindReference(condition, inputSchema))
    }
    columnarFilter
  }

  override def close(): Unit = {
    if (columnarFilter != null) {
      columnarFilter.close()
    }
  }
}
