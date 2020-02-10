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

package com.intel.sparkColumnarPlugin.expression

import java.util.concurrent.TimeUnit._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric


import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import scala.collection.mutable.ListBuffer
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.spark.sql.execution.vectorized.ArrowWritableColumnVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.evaluator._
import org.apache.spark.sql.util.ArrowUtils

import io.netty.buffer.ArrowBuf
import com.google.common.collect.Lists;

import org.apache.spark.sql.types.{DataType, StructType}
import com.intel.sparkColumnarPlugin.vectorized.ExpressionEvaluator
import com.intel.sparkColumnarPlugin.vectorized.BatchIterator

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
class ColumnarShuffledHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    resultSchema: StructType,
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    buildTime: SQLMetric,
    joinTime: SQLMetric,
    totalOutputNumRows: SQLMetric
    ) extends Logging{

    val inputBatchHolder = new ListBuffer[ArrowRecordBatch]() 
    // TODO
    val l_input_schema: Seq[Attribute] = left.output
    val r_input_schema: Seq[Attribute] = right.output

    val l_input_field_list: List[Field] = l_input_schema.toList.map(attr => {
      Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
    })
    val r_input_field_list: List[Field] = r_input_schema.toList.map(attr => {
      Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
    })

    val l_input_arrow_schema: Schema = new Schema(l_input_field_list.asJava)
    val r_input_arrow_schema: Schema = new Schema(r_input_field_list.asJava)

    logInfo(s"\nleftKeys is ${leftKeys}, \nrightKeys is ${rightKeys}, \nresultSchema is ${resultSchema}, \njoinType is ${joinType}, \ncondition is ${condition}")

    logInfo(s"\nleft input schema is ${l_input_schema}, \nright input schema is ${r_input_schema}, \nl_input_field_list is ${l_input_field_list}, \nr_input_field_list is ${r_input_field_list}")

    val l_key_expr_list = bindReferences(leftKeys, l_input_schema)
    val r_key_expr_list = bindReferences(rightKeys, r_input_schema)


    val lkeyFieldList: List[Field] = leftKeys.toList.map(expr => {
        val attr = ConverterUtils.getAttrFromExpr(expr)
        Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(expr.dataType))
    })

    val rkeyFieldList: List[Field] = rightKeys.toList.map(expr => {
        val attr = ConverterUtils.getAttrFromExpr(expr)
        Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(expr.dataType))
    })
    
    val r_key_indices = r_key_expr_list.toList.map(expr => expr.asInstanceOf[BoundReference].ordinal)

    val r_key_arrow_schema: Schema = new Schema(rkeyFieldList.asJava)
    logInfo(s"\nl_key_expr_list is ${l_key_expr_list}, \nr_key_expr_list is ${r_key_expr_list}, \nlkeyFieldList is ${lkeyFieldList}, \nrkeyFieldList is ${rkeyFieldList}")

    // only support single primary key here
    if (lkeyFieldList.size != 1) {
      throw new UnsupportedOperationException("Only support single join key currently.")
    }

    /////////////////////////////// Create Prober /////////////////////////////
    // Prober is used to insert left table primary key into hashMap
    // Then use iterator to probe right table primary key from hashmap
    // to get corresponding indices of left table
    //
    var prober = new ExpressionEvaluator()
    val probe_node = TreeBuilder.makeFunction(
      "probeArraysInner", 
      lkeyFieldList.map(field => {
        TreeBuilder.makeField(field)
      }).asJava, 
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val retType = Field.nullable("res", new ArrowType.Int(32, true))
    val probe_expr = TreeBuilder.makeExpression(probe_node, retType)

    prober.build(l_input_arrow_schema, Lists.newArrayList(probe_expr), true)

    /////////////////////////////// Create Shuffler /////////////////////////////
    // Shuffler will use input indices array to shuffle current table
    // output a new table with new sequence.
    var left_shuffler = new ExpressionEvaluator()
    val left_shuffle_node = TreeBuilder.makeFunction(
      "shuffleArrayList", 
      l_input_field_list.map(field => {
        TreeBuilder.makeField(field)
      }).asJava, 
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val l_action_expr_list = l_input_field_list.map(field => {
      TreeBuilder.makeExpression(
        TreeBuilder.makeFunction(
          "action_dono",
          Lists.newArrayList(
            left_shuffle_node,
            TreeBuilder.makeField(field)),
          field.getType),
        field)
    }).asJava

    left_shuffler.build(l_input_arrow_schema, l_action_expr_list, true)

    var right_shuffler = new ExpressionEvaluator()
    val right_shuffle_node = TreeBuilder.makeFunction(
      "shuffleArrayList", 
      r_input_field_list.map(field => {
        TreeBuilder.makeField(field)
      }).asJava, 
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val r_action_expr_list = r_input_field_list.map(field => {
      TreeBuilder.makeExpression(
        TreeBuilder.makeFunction(
          "action_dono",
          Lists.newArrayList(
            right_shuffle_node,
            TreeBuilder.makeField(field)),
          field.getType),
        field)
    }).asJava

    right_shuffler.build(r_input_arrow_schema, r_action_expr_list, false)

    var probe_iterator: BatchIterator = _
    var left_shuffle_iterator: BatchIterator = _

  def columnarInnerJoin(
    streamIter: Iterator[ColumnarBatch],
    buildIter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {

    val beforeBuild = System.nanoTime()
    var build_cb : ColumnarBatch = null
    while (buildIter.hasNext) {
      if (build_cb != null) {
        build_cb.close()
      }
      build_cb = buildIter.next()
      val build_rb = ConverterUtils.createArrowRecordBatch(build_cb)
      inputBatchHolder += build_rb
      prober.evaluate(build_rb)
      left_shuffler.evaluate(build_rb)
    }

    probe_iterator = prober.finishByIterator();
    left_shuffler.setDependency(probe_iterator, 0)
    right_shuffler.setDependency(probe_iterator, 1)
    left_shuffle_iterator = left_shuffler.finishByIterator();
    buildTime += NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)

    var last_cb: ColumnarBatch = null
    
    streamIter.map(cb => {
      if (last_cb != null) {
        last_cb.close()
      }
      val beforeJoin = System.nanoTime()
      last_cb = cb
      val r_key_cols = r_key_indices.map(i => {
        cb.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
      })
      val process_input_rb: ArrowRecordBatch = ConverterUtils.createArrowRecordBatch(cb.numRows, r_key_cols)
      val right_rb: ArrowRecordBatch = ConverterUtils.createArrowRecordBatch(cb)
      probe_iterator.processAndCacheOne(r_key_arrow_schema, process_input_rb)
      val l_output_rb: ArrowRecordBatch = left_shuffle_iterator.next()
      val r_output_rb: ArrowRecordBatch = right_shuffler.evaluate(right_rb)(0)
      val outputNumRows = r_output_rb.getLength()

      val l_output = ConverterUtils.fromArrowRecordBatch(l_input_arrow_schema, l_output_rb)
      val r_output = ConverterUtils.fromArrowRecordBatch(r_input_arrow_schema, r_output_rb)

      // TODO
      val resultColumnVectorList = l_output.toList ::: r_output.toList
      ConverterUtils.releaseArrowRecordBatch(process_input_rb)
      ConverterUtils.releaseArrowRecordBatch(right_rb)
      ConverterUtils.releaseArrowRecordBatch(l_output_rb)
      ConverterUtils.releaseArrowRecordBatch(r_output_rb)
      totalOutputNumRows += outputNumRows
      joinTime += NANOSECONDS.toMillis(System.nanoTime() - beforeJoin)
      new ColumnarBatch(resultColumnVectorList.map(v => v.asInstanceOf[ColumnVector]).toArray, outputNumRows)
    })
  }

  def close(): Unit = {
    ConverterUtils.releaseArrowRecordBatchList(inputBatchHolder.toArray)
    if (prober != null) {
      prober.close()
      prober = null
    }
    if (left_shuffler != null) {
      left_shuffler.close()
      left_shuffler = null
    }
    if (right_shuffler != null) {
      right_shuffler.close()
      right_shuffler = null
    }
    if (probe_iterator != null) {
      probe_iterator.close()
      probe_iterator = null
    }
    if (left_shuffle_iterator != null) {
      left_shuffle_iterator.close()
      left_shuffle_iterator = null
    }
  }

}

object ColumnarShuffledHashJoin {
  var columnarShuffedHahsJoin: ColumnarShuffledHashJoin = _
  def create(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    resultSchema: StructType,
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    buildTime: SQLMetric,
    joinTime: SQLMetric,
    numOutputRows: SQLMetric
    ): ColumnarShuffledHashJoin = synchronized {
    columnarShuffedHahsJoin = new ColumnarShuffledHashJoin(leftKeys, rightKeys, resultSchema, joinType, condition, left, right, buildTime, joinTime, numOutputRows)
    columnarShuffedHahsJoin
  }

  def close(): Unit = {
    if (columnarShuffedHahsJoin != null) {
      columnarShuffedHahsJoin.close()
    }
  }
}
