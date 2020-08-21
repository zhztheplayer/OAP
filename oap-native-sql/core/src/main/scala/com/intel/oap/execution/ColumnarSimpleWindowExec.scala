/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.oap.execution

import com.google.common.base.Preconditions
import com.intel.oap.expression.{ColumnarAggregation, ConverterUtils}
import com.intel.oap.vectorized.{ArrowWritableColumnVector, CloseableColumnBatchIterator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression, SortOrder, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.TaskContext
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, VarcharType}
import org.apache.spark.sql.util.ArrowUtils

import scala.collection.mutable.ListBuffer

class ColumnarSimpleWindowExec(windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan) extends WindowExec(windowExpression,
  partitionSpec, orderSpec, child) {

  override def supportsColumnar = true

  override def output: Seq[Attribute] = child.output ++ windowExpression.map(_.toAttribute)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in aggregation process"),
    "totalTime" -> SQLMetrics
        .createTimingMetric(sparkContext, "totaltime_hashagg"))

  val numOutputRows = longMetric("numOutputRows")
  val numOutputBatches = longMetric("numOutputBatches")
  val numInputBatches = longMetric("numInputBatches")
  val aggTime = longMetric("aggTime")
  val totalTime = longMetric("totalTime")

  val sparkConf = sparkContext.getConf

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // TODO
    child.executeColumnar().mapPartitionsWithIndex { (partIndex, iter) =>

      if (!iter.hasNext) {
        Iterator.empty
      } else {
        val groupingExpressions = partitionSpec.map(e => e.asInstanceOf[AttributeReference])

        val aggregateExpressions = windowExpression
            .map(e => e.asInstanceOf[Alias])
            .map(a => a.child.asInstanceOf[WindowExpression])
            .map(w => w.windowFunction.asInstanceOf[AggregateExpression])

        val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)

        val aggregateOutput = groupingExpressions ++ aggregateAttributes

        val aggregation = ColumnarAggregation.create(partIndex,
          groupingExpressions,
          child.output,
          aggregateExpressions,
          aggregateAttributes,
          aggregateOutput,
          aggregateOutput,
          numInputBatches,
          numOutputBatches,
          numOutputRows,
          aggTime,
          totalTime,
          sparkConf)

        TaskContext
            .get()
            .addTaskCompletionListener[Unit](_ => {
              aggregation.close()
            })
        val inputCache = new ListBuffer[ColumnarBatch]()
        val inputInterceptor = new Iterator[ColumnarBatch] {

          override def hasNext: Boolean = iter.hasNext

          override def next(): ColumnarBatch = {
            val batch = iter.next()
            inputCache += batch
            (0 until batch.numCols()).map(batch.column)
                .foreach(_.asInstanceOf[ArrowWritableColumnVector].retain())
            batch
          }
        }

        val groupingColumnOrdinalsOfInput = groupingExpressions.map { ge =>
          child.schema.fieldIndex(ge.name)
        }

        val aggOutputSchema = new StructType(
          aggregateOutput.map(e => StructField(e.asInstanceOf[NamedExpression].name, e.dataType)).toArray)

        val groupingColumnOrdinalsOfAggResult = groupingExpressions.map { ge =>
          aggOutputSchema.fieldIndex(ge.name)
        }
        val aggColumnOrdinalsOfAggResult = aggregateAttributes.map { ge =>
          aggOutputSchema.fieldIndex(ge.name)
        }

        val groupingValuesToAggValuesMap = scala.collection.mutable.Map[List[AnyRef], List[AnyRef]]()
        aggregation.createIterator(inputInterceptor).foreach {
          b =>
            (0 until b.numRows()).foreach { i =>
              val row = b.getRow(i)
              val groupingValues = groupingColumnOrdinalsOfAggResult.map { o =>
                val valueType = aggOutputSchema.fields(o).dataType
                row.get(o, valueType)
              }.toList

              val aggValues = aggColumnOrdinalsOfAggResult.map { o =>
                val valueType = aggOutputSchema.fields(o).dataType
                row.get(o, valueType)
              }.toList
              groupingValuesToAggValuesMap += (groupingValues -> aggValues)
            }
        }


        val aggSchema = new StructType(
          aggregateAttributes.map(
            e => StructField(e.asInstanceOf[NamedExpression].name, e.dataType)).toArray)
        inputCache.toList.map { b =>
          val windowValueVectors = ArrowWritableColumnVector.allocateColumns(b.numRows(),
            aggSchema)

          (0 until b.numRows()).foreach { rowOrdinal =>
            val row = b.getRow(rowOrdinal)
            val groupingValues = groupingColumnOrdinalsOfInput.map { o =>
              val valueType = child.schema.fields(o).dataType
              row.get(o, valueType)
            }.toList
            val aggValues = groupingValuesToAggValuesMap(groupingValues)
            windowValueVectors.zipWithIndex.foreach { case (v, i) =>
              val aggValue = aggValues(i)
              putAny(v, rowOrdinal, aggValue)
            }
          }
          windowValueVectors.foreach(_.setValueCount(b.numRows()))
          new ColumnarBatch((0 until b.numCols())
              .map(i => b.column(i)).toArray ++ windowValueVectors, b.numRows())
        }.toIterator
      }
    }
  }

  def putAny(v: ArrowWritableColumnVector, rowOrdinal: Int, aggValue: AnyRef): Unit = {
    v.dataType() match {
      case _: BooleanType => v.putBoolean(rowOrdinal, aggValue.asInstanceOf[Boolean])
      case _: ByteType => v.putByte(rowOrdinal, aggValue.asInstanceOf[Byte])
      case _: ShortType => v.putShort(rowOrdinal, aggValue.asInstanceOf[Short])
      case _: IntegerType => v.putInt(rowOrdinal, aggValue.asInstanceOf[Int])
      case _: LongType => v.putLong(rowOrdinal, aggValue.asInstanceOf[Long])
      case _: FloatType => v.putFloat(rowOrdinal, aggValue.asInstanceOf[Float])
      case _: DoubleType => v.putDouble(rowOrdinal, aggValue.asInstanceOf[Double])
      case _ => throw new UnsupportedOperationException("Unsupported type: " + v.dataType())
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ColumnarSimpleWindowExec]

  override def equals(other: Any): Boolean = other match {
    case that: ColumnarSimpleWindowExec =>
      (that canEqual this) && (that eq this)
    case _ => false
  }

  override def hashCode(): Int = System.identityHashCode(this)

}