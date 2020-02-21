package com.intel.sparkColumnarPlugin.expression

import java.util.Collections
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

class ColumnarConditionProjector(
  condExpr: Expression,
  projExprs: Seq[Expression],
  originalInputAttributes: Seq[Attribute],
  numInputBatches: SQLMetric,
  numOutputBatches: SQLMetric,
  numOutputRows: SQLMetric,
  procTime: SQLMetric)
  extends Logging {
  logInfo(s"\nCondition is ${condExpr}, \nProjection is ${projExprs}")
  var elapseTime_make: Long = 0
  val start_make: Long = System.nanoTime()
  var skip = false

  val conditionFieldList : java.util.List[Field] = Lists.newArrayList()
  val condPrepareList: (TreeNode, ArrowType) = if (condExpr != null) {
    val columnarCondExpr: Expression = ColumnarExpressionConverter.replaceWithColumnarExpression(condExpr)
    val (cond, resultType) =
      columnarCondExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(conditionFieldList)
    (cond, resultType)
  } else {
    null
  }
  Collections.sort(conditionFieldList, (l: Field, r: Field) => { l.getName.compareTo(r.getName)})
  val conditionOrdinalList: List[Int] = conditionFieldList.asScala.toList.map(field => {
    field.getName.replace("c_", "").toInt
  })

  var projectFieldList : java.util.List[Field] = Lists.newArrayList()
  var projPrepareList : Seq[(ExpressionTree, ArrowType)] = null
  if (projExprs != null) {
    val columnarProjExprs: Seq[Expression] = projExprs.map(expr => {
      ColumnarExpressionConverter.replaceWithColumnarExpression(expr)
    })
    projPrepareList = columnarProjExprs.map(columnarExpr => {
      val (node, resultType) =
        columnarExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(projectFieldList)
      val result = Field.nullable("result", resultType)
      (TreeBuilder.makeExpression(node, result), resultType)
    })
  }

  if (projectFieldList.size == 0) { 
    if (conditionFieldList.size > 0) {
      projectFieldList = originalInputAttributes.zipWithIndex.toList.map{case (attr, i) => 
        Field.nullable(s"c_${i}", CodeGeneration.getResultType(attr.dataType))
      }.asJava
    } else {
      logInfo(s"skip this conditionprojection")
      skip = true
    }
  } else {
    // sort projectFieldList by ordinal
    Collections.sort(projectFieldList, (l: Field, r: Field) => { l.getName.compareTo(r.getName)})
  }
  val projectOrdinalList: List[Int] = projectFieldList.asScala.toList.map(field => {
    field.getName.replace("c_", "").toInt
  })

  val projectResultFieldList = if (projPrepareList != null) {
    projPrepareList.map(expr => Field.nullable(s"result", expr._2)).toList.asJava
  } else {
    projPrepareList = 
      projectFieldList.asScala.map(field => {
        (TreeBuilder.makeExpression(TreeBuilder.makeField(field), field), field.getType)
      })
    projectFieldList
  }

  val conditionArrowSchema = new Schema(conditionFieldList)
  val projectionArrowSchema = new Schema(projectFieldList)
  val projectionSchema = ArrowUtils.fromArrowSchema(projectionArrowSchema)
  val resultArrowSchema = new Schema(projectResultFieldList)
  val resultSchema = ArrowUtils.fromArrowSchema(resultArrowSchema)
  logInfo(s"conditionArrowSchema is ${conditionArrowSchema}, conditionOrdinalList is ${conditionOrdinalList}, \nprojectionArrowSchema is ${projectionArrowSchema}, projectionOrinalList is ${projectOrdinalList}, \nresult schema is ${resultArrowSchema}")

  val conditioner = if (skip == false && condPrepareList != null) {
    createFilter(conditionArrowSchema, condPrepareList)
  } else {
    null
  }
  val withCond: Boolean = if (conditioner != null) {
    true
  } else {
    false
  }
  val projector = if (skip == false) {
    createProjector(projectionArrowSchema, projPrepareList, withCond)
  } else {
    null
  }

  elapseTime_make = System.nanoTime() - start_make
  logInfo(s"Gandiva make total ${TimeUnit.NANOSECONDS.toMillis(elapseTime_make)} ms.")

  val allocator = new RootAllocator(9223372036854775807L)

  def createFilter(arrowSchema: Schema, prepareList: (TreeNode, ArrowType)): Filter =
    synchronized {
      if (conditioner != null) {
        return conditioner
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

  def createStructType(arrowSchema: Schema): StructType = {
    ArrowUtils.fromArrowSchema(arrowSchema)
  }

  def close(): Unit = {
    allocator.close()
    if (conditioner != null) {
      conditioner.close()
    }
    if (projector != null) {
      projector.close()
    }
  }

  def createIterator(cbIterator: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      private var columnarBatch: ColumnarBatch = null
      private var resColumnarBatch: ColumnarBatch = null
      private var nextCalled = false

      override def hasNext: Boolean = {
        if (!nextCalled && resColumnarBatch != null) {
          return true
        }
        nextCalled = false
        if (columnarBatch != null) {
          columnarBatch.close()
          columnarBatch = null
        }
        if (cbIterator.hasNext) {
          columnarBatch = cbIterator.next()
          numInputBatches += 1
        } else {
          resColumnarBatch = null
          return false
        }
        val beforeEval: Long = System.nanoTime()
        if (skip == true){
          columnarBatch.retain()
          resColumnarBatch = columnarBatch
          return true
        } 
        while (columnarBatch.numRows() == 0) {
          logInfo(s"$cbIterator Got empty ColumnarBatch")
          if (columnarBatch != null) {
            columnarBatch.close()
            columnarBatch = null
          }

          if (cbIterator.hasNext) {
            columnarBatch = cbIterator.next()
            numInputBatches += 1
          } else {
            resColumnarBatch = null
            logInfo(s"$cbIterator has no next, return false")
            return false
          }
        }

        // for now, we get a columnarBatch contains data
        var numRows = if (conditioner != null) {
          0
        } else {
          columnarBatch.numRows
        }
        val selectionBuffer = allocator.buffer(columnarBatch.numRows() * 2)
        val selectionVector = new SelectionVectorInt16(selectionBuffer)
        var input : ArrowRecordBatch = null
        while (numRows == 0) {
          // do conditioner here
          numRows = columnarBatch.numRows
          val cols = conditionOrdinalList.map(i => {
            columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
          })
          input = ConverterUtils.createArrowRecordBatch(numRows, cols)
          conditioner.evaluate(input, selectionVector)
          numRows = selectionVector.getRecordCount()
          if (numRows == 0) {
            // fetch next batch
            logInfo(s"$cbIterator filter got empty ColumnarBatch")
            if (columnarBatch != null) {
              ConverterUtils.releaseArrowRecordBatch(input)
              columnarBatch.close()
              columnarBatch = null
            }
  
            if (cbIterator.hasNext) {
              columnarBatch = cbIterator.next()
              numInputBatches += 1
            } else {
              // all columnarBatch has been fetched, no next
              selectionBuffer.close()
              resColumnarBatch = null
              logInfo(s"$cbIterator has no conditioned rows, return false")
              return false
            }
          }
          ConverterUtils.releaseArrowRecordBatch(input)
        }
  
        // for now, we either filter one columnarBatch who has valid rows or we only need to do project
        // either scenario we will need to output one columnarBatch.
        val resultColumnVectors = ArrowWritableColumnVector.allocateColumns(numRows, resultSchema).toArray
        val outputVectors = resultColumnVectors.map(columnVector => {
          columnVector.getValueVector()
        }).toList.asJava
  
        val cols = projectOrdinalList.map(i => {
          columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
        })
        input = ConverterUtils.createArrowRecordBatch(columnarBatch.numRows, cols)
        if(conditioner != null) {
          projector.evaluate(input, selectionVector, outputVectors);
        } else {
          projector.evaluate(input, outputVectors);
        }
  
        ConverterUtils.releaseArrowRecordBatch(input)
        selectionBuffer.close()
  
        val outputBatch = new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), numRows)
        procTime += ((System.nanoTime() - beforeEval) / (1000 * 1000))
        resColumnarBatch = outputBatch
        true
      }

      override def next(): ColumnarBatch = {
        nextCalled = true
        if (resColumnarBatch == null) {
          throw new UnsupportedOperationException("Iterator has no next columnar batch or it hasn't been called by hasNext.")
        }
        numOutputBatches += 1
        numOutputRows += resColumnarBatch.numRows
        resColumnarBatch
      }

    }// end of Iterator
  }// end of createIterator

}// end of class

object ColumnarConditionProjector {
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
      inputSchema,
      numInputBatches,
      numOutputBatches,
      numOutputRows,
      procTime)
  }
}
