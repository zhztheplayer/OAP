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
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}


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
    buildSide: BuildSide,
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

    val resultFieldList = resultSchema.map(field => {
      Field.nullable(field.name, CodeGeneration.getResultType(field.dataType))
    }).toList

    val lkeyFieldList: List[Field] = leftKeys.toList.map(expr => {
        val attr = ConverterUtils.getAttrFromExpr(expr)
        Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(expr.dataType))
    })

    val rkeyFieldList: List[Field] = rightKeys.toList.map(expr => {
        val attr = ConverterUtils.getAttrFromExpr(expr)
        Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(expr.dataType))
    })
    
    val (build_key_field_list, stream_key_field_list, stream_key_ordinal_list, build_input_field_list, stream_input_field_list)
      = buildSide match {
      case BuildLeft =>
        val stream_key_expr_list = bindReferences(rightKeys, r_input_schema) 
        (lkeyFieldList, rkeyFieldList, stream_key_expr_list.toList.map(_.asInstanceOf[BoundReference].ordinal), l_input_field_list, r_input_field_list)

      case BuildRight =>
        val stream_key_expr_list = bindReferences(leftKeys, l_input_schema) 
        (rkeyFieldList, lkeyFieldList, stream_key_expr_list.toList.map(_.asInstanceOf[BoundReference].ordinal), r_input_field_list, l_input_field_list)

    }

    val (build_output_field_list, stream_output_field_list) = joinType match {
      case _: InnerLike =>
        (build_input_field_list, stream_input_field_list)
      case LeftSemi =>
        (List[Field](), stream_input_field_list)
      case _ =>
        throw new UnsupportedOperationException(s"Join Type ${joinType} is not supported yet.")
    }

    val build_input_arrow_schema: Schema = new Schema(build_input_field_list.asJava)
    val stream_input_arrow_schema: Schema = new Schema(stream_input_field_list.asJava)

    val build_output_arrow_schema: Schema = new Schema(build_output_field_list.asJava)
    val stream_output_arrow_schema: Schema = new Schema(stream_output_field_list.asJava)

    val stream_key_arrow_schema: Schema = new Schema(stream_key_field_list.asJava)

    logInfo(s"\nleft_schema is ${l_input_schema}, \nright_schema is ${r_input_schema}, \nresultSchema is ${resultSchema}, \njoinType is ${joinType}, \nbuildSide is ${buildSide}, \ncondition is ${condition}")

    logInfo(s"\nbuild_key_field_list is ${build_key_field_list}, stream_key_field_list is ${stream_key_field_list}, stream_key_ordinal_list is ${stream_key_ordinal_list}, \nbuild_input_field_list is ${build_input_field_list}, stream_input_field_list is ${stream_input_field_list}, \nbuild_output_field_list is ${build_output_field_list}, stream_output_field_list is ${stream_output_field_list}")

    // only support single primary key here
    if (build_key_field_list.size != 1) {
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
      build_key_field_list.map(field => {
        TreeBuilder.makeField(field)
      }).asJava, 
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val retType = Field.nullable("res", new ArrowType.Int(32, true))
    val probe_expr = TreeBuilder.makeExpression(probe_node, retType)

    prober.build(build_input_arrow_schema, Lists.newArrayList(probe_expr), true)

    /////////////////////////////// Create Shuffler /////////////////////////////
    // Shuffler will use input indices array to shuffle current table
    // output a new table with new sequence.
    var build_shuffler = new ExpressionEvaluator()
    val build_shuffle_node = TreeBuilder.makeFunction(
      "shuffleArrayList", 
      build_input_field_list.map(field => {
        TreeBuilder.makeField(field)
      }).asJava, 
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val build_action_expr_list = build_input_field_list.map(field => {
      TreeBuilder.makeExpression(
        TreeBuilder.makeFunction(
          "action_dono",
          Lists.newArrayList(
            build_shuffle_node,
            TreeBuilder.makeField(field)),
          field.getType),
        field)
    }).asJava

    build_shuffler.build(build_input_arrow_schema, build_action_expr_list, true)

    var stream_shuffler = new ExpressionEvaluator()
    val stream_shuffle_node = TreeBuilder.makeFunction(
      "shuffleArrayList", 
      stream_input_field_list.map(field => {
        TreeBuilder.makeField(field)
      }).asJava, 
      new ArrowType.Int(32, true)/*dummy ret type, won't be used*/)
    val stream_action_expr_list = stream_input_field_list.map(field => {
      TreeBuilder.makeExpression(
        TreeBuilder.makeFunction(
          "action_dono",
          Lists.newArrayList(
            stream_shuffle_node,
            TreeBuilder.makeField(field)),
          field.getType),
        field)
    }).asJava

    stream_shuffler.build(stream_input_arrow_schema, stream_action_expr_list, false)

    var probe_iterator: BatchIterator = _
    var build_shuffle_iterator: BatchIterator = _

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
      build_shuffler.evaluate(build_rb)
    }

    probe_iterator = prober.finishByIterator();
    build_shuffler.setDependency(probe_iterator, 0)
    stream_shuffler.setDependency(probe_iterator, 1)
    build_shuffle_iterator = build_shuffler.finishByIterator();
    buildTime += NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)

    var last_cb: ColumnarBatch = null
    
    streamIter.map(cb => {
      if (last_cb != null) {
        last_cb.close()
      }
      val beforeJoin = System.nanoTime()
      last_cb = cb
      val stream_key_cols = stream_key_ordinal_list.map(i => {
        cb.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
      })
      val process_input_rb: ArrowRecordBatch = ConverterUtils.createArrowRecordBatch(cb.numRows, stream_key_cols)
      probe_iterator.processAndCacheOne(stream_key_arrow_schema, process_input_rb)

      val stream_rb: ArrowRecordBatch = ConverterUtils.createArrowRecordBatch(cb)
      val stream_output_rb: ArrowRecordBatch = stream_shuffler.evaluate(stream_rb)(0)
      val stream_output = ConverterUtils.fromArrowRecordBatch(stream_output_arrow_schema, stream_output_rb)
      ConverterUtils.releaseArrowRecordBatch(stream_rb)
      ConverterUtils.releaseArrowRecordBatch(stream_output_rb)
      val outputNumRows = stream_output_rb.getLength()

      val build_output = if (build_output_field_list.length > 0) {
        val build_output_rb: ArrowRecordBatch = build_shuffle_iterator.next()
        val build_output = ConverterUtils.fromArrowRecordBatch(build_output_arrow_schema, build_output_rb)
        ConverterUtils.releaseArrowRecordBatch(build_output_rb)
        build_output
      } else {
        Array[ArrowWritableColumnVector]()
      }
      // TODO
      val resultColumnVectorList = buildSide match {
        case BuildLeft =>
          build_output.toList ::: stream_output.toList
        case BuildRight =>
          stream_output.toList ::: build_output.toList
      }
      ConverterUtils.releaseArrowRecordBatch(process_input_rb)
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
    if (build_shuffler != null) {
      build_shuffler.close()
      build_shuffler = null
    }
    if (stream_shuffler != null) {
      stream_shuffler.close()
      stream_shuffler = null
    }
    if (probe_iterator != null) {
      probe_iterator.close()
      probe_iterator = null
    }
    if (build_shuffle_iterator != null) {
      build_shuffle_iterator.close()
      build_shuffle_iterator = null
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
    buildSide: BuildSide, 
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    buildTime: SQLMetric,
    joinTime: SQLMetric,
    numOutputRows: SQLMetric
    ): ColumnarShuffledHashJoin = synchronized {
    columnarShuffedHahsJoin = new ColumnarShuffledHashJoin(leftKeys, rightKeys, resultSchema, joinType, buildSide, condition, left, right, buildTime, joinTime, numOutputRows)
    columnarShuffedHahsJoin
  }

  def close(): Unit = {
    if (columnarShuffedHahsJoin != null) {
      columnarShuffedHahsJoin.close()
    }
  }
}
