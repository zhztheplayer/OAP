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
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
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

class ColumnarConditionProjector(
  condExpr: Expression,
  projExprs: Seq[Expression],
  numInputBatches: SQLMetric,
  numOutputBatches: SQLMetric,
  numOutputRows: SQLMetric,
  procTime: SQLMetric) extends (ColumnarBatch => ColumnarBatch)
    with AutoCloseable
    with Logging {
  // build gandiva projection here.
  var elapseTime_make: Long = 0
  val start_make: Long = System.nanoTime()
  var skip = false

  val fieldTypesList : java.util.List[Field] = Lists.newArrayList()
  val condPrepareList: (TreeNode, ArrowType) = if (condExpr != null) {
    val columnarCondExpr: Expression = ColumnarExpressionConverter.replaceWithColumnarExpression(condExpr)
    val (cond, resultType) =
      columnarCondExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(fieldTypesList)
    (cond, resultType)
  } else {
    null
  }

  val projPrepareList : Seq[(ExpressionTree, ArrowType)] = if (projExprs != null) {
    val columnarProjExprs: Seq[Expression] = projExprs.map(expr => {
      ColumnarExpressionConverter.replaceWithColumnarExpression(expr)
    })
    columnarProjExprs.map(columnarExpr => {
      val (node, resultType) =
        columnarExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(fieldTypesList)
      val result = Field.nullable("result", resultType)
      (TreeBuilder.makeExpression(node, result), resultType)
    })
  } else {
    fieldTypesList.asScala.map(field => {
      (TreeBuilder.makeExpression(TreeBuilder.makeField(field), field), field.getType)
    })
  }

  if (fieldTypesList.size == 0) {
    logInfo(s"Do not need any convertion in this operator, skip")
    skip = true
  }
  // sort fieldTypesList by ordinal
  Collections.sort(fieldTypesList, (l: Field, r: Field) => { l.getName.compareTo(r.getName)})

  val arrowSchema = new Schema(fieldTypesList)
  val schema = ArrowUtils.fromArrowSchema(arrowSchema)
  val resultArrowSchema = new Schema(
    projPrepareList.map(expr => Field.nullable(s"result", expr._2)).toList.asJava)
  val resultSchema = ArrowUtils.fromArrowSchema(resultArrowSchema)
  logInfo(s"arrowSchema is $arrowSchema, result schema is $resultArrowSchema")

  val filter = if (skip == false && condPrepareList != null) {
    createFilter(arrowSchema, condPrepareList)
  } else {
    null
  }
  val withCond: Boolean = if (filter != null) {
    true
  } else {
    false
  }
  val projector = if (skip == false) {
    createProjector(arrowSchema, projPrepareList, withCond)
  } else {
    null
  }

  elapseTime_make = System.nanoTime() - start_make
  logInfo(s"Gandiva make total ${TimeUnit.NANOSECONDS.toMillis(elapseTime_make)} ms.")

  val allocator = new RootAllocator(Long.MAX_VALUE)

  def initialize(): Unit = {}

  override def close(): Unit = {
    allocator.close()
    if (filter != null) {
      filter.close()
    }
    if (projector != null) {
      projector.close()
    }
  }

  def apply(columnarBatch: ColumnarBatch): ColumnarBatch = {
    /*for (i <- 0 until columnarBatch.numCols) {
      logInfo(s"col ordinal is $i, type is ${columnarBatch.column(i).dataType}")
    }*/

    numInputBatches += 1
    val beforeEval: Long = System.nanoTime()
    val outputBatch = if (skip != true && columnarBatch.numRows() != 0) {
      val eval_start = System.nanoTime()
      val input = ConverterUtils.createArrowRecordBatch(columnarBatch)
      val selectionBuffer = allocator.buffer(columnarBatch.numRows() * 2)
      val selectionVector = new SelectionVectorInt16(selectionBuffer)
      val numRows = if (filter != null) {
        filter.evaluate(input, selectionVector)
        selectionVector.getRecordCount()
      } else {
        columnarBatch.numRows
      }

      val resultColumnVectors = ArrowWritableColumnVector
        .allocateColumns(numRows, resultSchema)
        .toArray
      val outputVectors =
        resultColumnVectors.map(columnVector =>
          columnVector.getValueVector()).toList.asJava

      if(filter != null) {
        projector.evaluate(input, selectionVector, outputVectors);
      } else {
        projector.evaluate(input, outputVectors);
      }

      ConverterUtils.releaseArrowRecordBatch(input)
      selectionBuffer.close()

      numOutputRows += numRows
      new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), numRows)
    } else if (skip == true){
      columnarBatch.retain()
      columnarBatch
    } else {
      val resultColumnVectors =
        ArrowWritableColumnVector.allocateColumns(columnarBatch.numRows(), schema).toArray
      new ColumnarBatch(
        resultColumnVectors.map(_.asInstanceOf[ColumnVector]),
        columnarBatch.numRows())
    }
    procTime += ((System.nanoTime() - beforeEval) / (1000 * 1000))
    numOutputBatches += 1
    outputBatch
  }

  def createFilter(arrowSchema: Schema, prepareList: (TreeNode, ArrowType)): Filter =
    synchronized {
      if (filter != null) {
        return filter
      }
      Filter.make(arrowSchema, TreeBuilder.makeCondition(prepareList._1))
    }

  def createProjector(
      arrowSchema: Schema,
      prepareList: Seq[(ExpressionTree, ArrowType)],
      withCond: Boolean): Projector = synchronized {
    if (projector != null) {
      return projector
    }
    val fieldNodesList = prepareList.map(_._1).toList.asJava
    if (withCond) {
      Projector.make(arrowSchema, fieldNodesList, SelectionVectorType.SV_INT16)
    } else {
      Projector.make(arrowSchema, fieldNodesList)
    }
  }

  def createStructType(arrowSchema: Schema): StructType = synchronized {
    ArrowUtils.fromArrowSchema(arrowSchema)
  }

}

object ColumnarConditionProjector extends AutoCloseable {

  //var columnarCondProj: ColumnarConditionProjector = _
  def create(
    condition: Expression,
    exprs: Seq[Expression],
    inputSchema: Seq[Attribute],
    numInputBatches: SQLMetric,
    numOutputBatches: SQLMetric,
    numOutputRows: SQLMetric,
    procTime: SQLMetric): ColumnarConditionProjector = synchronized {
    val conditionExpr = if (condition != null) {
      BindReferences.bindReference(condition, inputSchema)
    } else {
      null
    }
    val projectListExpr = if (exprs != null) {
      bindReferences(exprs, inputSchema)
    } else {
      null
    }
    new ColumnarConditionProjector(
      conditionExpr,
      projectListExpr,
      numInputBatches,
      numOutputBatches,
      numOutputRows,
      procTime)
  }

  override def close(): Unit = {
    /*if (columnarCondProj != null) {
      columnarCondProj.close()
    }*/
  }
}
