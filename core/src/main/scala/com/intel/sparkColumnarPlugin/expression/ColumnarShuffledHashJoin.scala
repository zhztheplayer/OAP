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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics


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

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
class ColumnarShuffledHashJoin(
    resultSchema: StructType,
    joinType: JoinType,
    condition: Option[Expression]) extends Logging{
    val schema = resultSchema

  def columnarInnerJoin(
      streamIter: Iterator[ColumnarBatch],
      buildIter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {

      val a = Field.nullable("a", new ArrowType.Int(32, true))
      val args = Lists.newArrayList(TreeBuilder.makeField(a))
      val retType = Field.nullable("c", new ArrowType.Int(32, true))
      val cols = Lists.newArrayList(a)
      val bschema = new Schema(cols)

      val prober = new ExpressionEvaluator()
      val isin_node = TreeBuilder.makeFunction("probeArray", args, new ArrowType.Int(32, true))
      val isin_root = TreeBuilder.makeExpression(isin_node, retType)
      prober.build(bschema, Lists.newArrayList(isin_root), true)

      val appender = new ExpressionEvaluator()
      val append_node = TreeBuilder.makeFunction("append", args, new ArrowType.Int(32, true))
      val append_root = TreeBuilder.makeExpression(append_node, retType)
      appender.build(bschema, Lists.newArrayList(append_root), true)

      val appender_rb = new ExpressionEvaluator()
      val append_node_rb = TreeBuilder.makeFunction("append", args, new ArrowType.Int(32, true))
      val append_root_rb = TreeBuilder.makeExpression(append_node_rb, retType)
      appender_rb.build(bschema, Lists.newArrayList(append_root_rb), true)


      val taker = new ExpressionEvaluator()
      val taker_node = TreeBuilder.makeFunction("takeArray", args, new ArrowType.Int(32, true))
      val taker_root = TreeBuilder.makeExpression(taker_node, retType)
      taker.build(bschema, Lists.newArrayList(taker_root), true)

      val ntaker = new ExpressionEvaluator()
      val ntaker_node = TreeBuilder.makeFunction("ntakeArray", args, new ArrowType.Int(32, true))
      val ntaker_root = TreeBuilder.makeExpression(ntaker_node, retType)
      ntaker.build(bschema, Lists.newArrayList(ntaker_root), true)




/*
      while (buildIter.hasNext) {
        val buildCb = buildIter.next()
        val rescolumns = ArrowWritableColumnVector.allocateColumns(buildCb.numRows(), ArrowUtils.fromArrowSchema(bschema)).toArray
        val outputVectors = rescolumns.map(columnVector => columnVector.getValueVector()).toList.asJava
        val input = createArrowRecordBatch(buildCb)
        eval.evaluate(input, outputVectors)

        val resultColumnarBatch = new ColumnarBatch(rescolumns.map(_.asInstanceOf[ColumnVector]), buildCb.numRows())
        
        logInfo(s"build cb: ${buildCb.numRows()}")
        logInfo(s"hash result row: ${resultColumnarBatch.numRows()}")
        logInfo(s"hash result: ${resultColumnarBatch.column(0)}")
      }

*/

    while (buildIter.hasNext) {
      val build_cb = buildIter.next()
      // save to a big record batch
      val build_rb = createArrowRecordBatch(build_cb)
      appender_rb.evaluate(build_rb)

      val hash_cb = calcHashOne(build_cb)

      val hash_rb = createArrowRecordBatch(hash_cb)
      appender.evaluate(hash_rb)

    }
    val full_hash_rblist = appender.finish()
    logInfo(s"ht combined batch ${full_hash_rblist(0)}")

    val final_build_rblist = appender_rb.finish()
    logInfo(s"build combined batch ${final_build_rblist(0)}")

    prober.SetMember(full_hash_rblist(0))


    //TODO: build entire hash table
    //val buildCB = buildIter.next()
    //val hashCB = calcHashOne(buildCB)
    //val ht = createArrowRecordBatch(hashCB)
    //logInfo(s"ht batch ${ht}")

    val localSchema = this.schema
    //val buildSchema = buildCB.schema

    streamIter.map(cb => {
      //val buildCb = cb
      //val input = createArrowRecordBatch(buildCb)
      //val resultRecordBatchList = eval.evaluate(input)
      //val rescolumns = ArrowWritableColumnVector.allocateColumns(buildCb.numRows(), ArrowUtils.fromArrowSchema(bschema)).toArray
      //val outputVectors = rescolumns.map(columnVector => columnVector.getValueVector()).toList.asJava
      //eval.evaluate(input, outputVectors)

      //val resultColumnarBatch = new ColumnarBatch(rescolumns.map(_.asInstanceOf[ColumnVector]), buildCb.numRows())
      logInfo(s"cb row: ${cb.numRows()}")

      ////probeBatch(resultColumnarBatch, resultColumnarBatch)

      val stream_orig_rb = createArrowRecordBatch(cb)

      val stream_hash_cb = calcHashOne(cb)
      val stream_hash_rb = createArrowRecordBatch(stream_hash_cb)

      //val build_record_batch = createArrowRecordBatch(buildCB)
      prober.evaluate(stream_hash_rb)
      val indexList = prober.finish()
      logInfo(s"index list: ${indexList(0)}")

      taker.SetMember(indexList(0))
      taker.evaluate(final_build_rblist(0))
      val left_list = taker.finish()
      logInfo(s"left result list: ${left_list(0)}")

      //val buildResultList = taker.evaluate(build_record_batch, indexList(0))
      ntaker.SetMember(indexList(0))
      ntaker.evaluate(stream_orig_rb)
      val right_list = ntaker.finish()
      logInfo(s"right result list: ${right_list(0)}")
      ////val joinedResultList = taker.evaluate(final_build_rblist(0))
      //val joinedResultList = ntaker.evaluate(stream_orig_rb)
      //val joinedResultList = ntaker.evaluate(final_build_rblist(0))
      
      //logInfo(s"stream result list: ${joinedResultList(0)}")

      //val joinedRecordBatch = combineArrowRecordBatch(buildResultList(0), streamResultList(0))

      //val resultColumnVectorList = fromArrowRecordBatch(ArrowUtils.toArrowSchema(localSchema, null), joinedRecordBatch)
      //val joinedCB = new ColumnarBatch(resultColumnVectorList.map(_.asInstanceOf[ColumnVector]), joinedRecordBatch.getLength())
      cb.retain()
      cb
    })

/*
    Field a = Field.nullable("a", int32);
    List<Field> args = Lists.newArrayList(a);

    Field retType = Field.nullable("c", int32);
    ExpressionTree root = TreeBuilder.makeExpression("hash32", args, retType);

*/

/*
    val joinRow = new JoinedRow
    val JoinKeys = tstreamSideKeyGenerator()
    val localSchema = this.schema
    streamIter.map( cb => {
      val converter = new RowToColumnConverter(localSchema)
      val columns = ArrowWritableColumnVector.allocateColumns(4096, localSchema)
      val batch = new ColumnarBatch(columns.toArray, 4096)

      var numRows = 0
      val input = cb.rowIterator().asScala
      while (input.hasNext) {
        val sRow = input.next()
        val matches = hashedRelation.get(JoinKeys(sRow))
        if (matches != null) {
          numRows += 1
          joinRow.withLeft(sRow)
          matches.map(joinRow.withRight(_))
          converter.convert(matches.next(), columns.toArray)
        }
      }
      batch.setNumRows(numRows)
      batch
    }
   )
*/
  }

  private def probeBatch(cb: ColumnarBatch, 
                         hashcb: ColumnarBatch) = {
/*
    val prober = new ExpressionEvaluator()
    val arrowSchema = new Schema(inputFieldList.asJava)
    if (keyFieldList.length > 0) {
    aggregator.build(
      arrowSchema,
      gandivaExpressionTree.map(_._1).asJava, true)
    } else {
    aggregator.build(
      arrowSchema,
      gandivaExpressionTree.map(_._1).asJava, gandivaExpressionTree.map(_._2).asJava)
    }
*/
    cb
  }

  private def calcHash(arbIter: Iterator[ColumnarBatch]): ColumnarBatch = {
    //val a = Field.nullable("a", int32)
    //List<Field> args = Lists.newArrayList(a);

    //Field retType = Field.nullable("c", int32);
    //ExpressionTree root = TreeBuilder.makeExpression("hash32", args, retType);

    //List<ExpressionTree> exprs = Lists.newArrayList(root);

    //Schema schema = new Schema(args);
    //Projector eval = Projector.make(schema, exprs);

      val a = Field.nullable("a", new ArrowType.Int(32, true))
      val args = Lists.newArrayList(TreeBuilder.makeField(a))
      val retType = Field.nullable("c", new ArrowType.Int(32, true))
      //val root = TreeBuilder.makeExpression("hash32", args, retType)
      val node = TreeBuilder.makeFunction("hash32", args, new ArrowType.Int(32, true))
      val root = TreeBuilder.makeExpression(node, retType)

      val cols = Lists.newArrayList(a)
      val bschema = new Schema(cols)
      val eval = Projector.make(bschema, Lists.newArrayList(root))

      if (arbIter.hasNext) {
        val buildCb = arbIter.next()
        val rescolumns = ArrowWritableColumnVector.allocateColumns(buildCb.numRows(), ArrowUtils.fromArrowSchema(bschema)).toArray
        val outputVectors = rescolumns.map(columnVector => columnVector.getValueVector()).toList.asJava
        val input = createArrowRecordBatch(buildCb)
        eval.evaluate(input, outputVectors)

        new ColumnarBatch(rescolumns.map(_.asInstanceOf[ColumnVector]), buildCb.numRows())
      }
      arbIter.next()
  }

  private def calcHashOne(cb: ColumnarBatch): ColumnarBatch = {
    //val a = Field.nullable("a", int32)
    //List<Field> args = Lists.newArrayList(a);

    //Field retType = Field.nullable("c", int32);
    //ExpressionTree root = TreeBuilder.makeExpression("hash32", args, retType);

    //List<ExpressionTree> exprs = Lists.newArrayList(root);

    //Schema schema = new Schema(args);
    //Projector eval = Projector.make(schema, exprs);

      val a = Field.nullable("a", new ArrowType.Int(32, true))
      val args = Lists.newArrayList(TreeBuilder.makeField(a))
      val retType = Field.nullable("c", new ArrowType.Int(32, true))
      //val root = TreeBuilder.makeExpression("hash32", args, retType)
      val node = TreeBuilder.makeFunction("hash32", args, new ArrowType.Int(32, true))
      val root = TreeBuilder.makeExpression(node, retType)

      val cols = Lists.newArrayList(a)
      val bschema = new Schema(cols)
      val eval = Projector.make(bschema, Lists.newArrayList(root))

      val buildCb = cb
      val rescolumns = ArrowWritableColumnVector.allocateColumns(buildCb.numRows(), ArrowUtils.fromArrowSchema(bschema)).toArray
      val outputVectors = rescolumns.map(columnVector => columnVector.getValueVector()).toList.asJava
      val input = createArrowRecordBatch(buildCb)
      eval.evaluate(input, outputVectors)

      new ColumnarBatch(rescolumns.map(_.asInstanceOf[ColumnVector]), buildCb.numRows())
  }

  private def createArrowRecordBatch(columnarBatch: ColumnarBatch): ArrowRecordBatch = {
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

  private def fromArrowRecordBatch(recordBatchSchema: Schema, recordBatch: ArrowRecordBatch): Array[ArrowWritableColumnVector] = {
    val numRows = recordBatch.getLength();
    ArrowWritableColumnVector.loadColumns(numRows, recordBatchSchema, recordBatch)
  }

  private def combineArrowRecordBatch(rb1: ArrowRecordBatch, rb2: ArrowRecordBatch): ArrowRecordBatch = {
     val numRows = rb1.getLength()
     val rb1_nodes = rb1.getNodes()
     val rb2_nodes = rb2.getNodes()
     val rb1_bufferlist = rb1.getBuffers()
     val rb2_bufferlist = rb2.getBuffers()

     val combined_nodes = rb1_nodes.addAll(rb2_nodes)
     val combined_bufferlist = rb1_bufferlist.addAll(rb2_bufferlist)
     new ArrowRecordBatch(numRows, rb1_nodes, rb1_bufferlist)
  }

  def releaseArrowRecordBatch(recordBatch: ArrowRecordBatch): Unit = {
    if (recordBatch != null)
      recordBatch.close()
  }

  def releaseArrowRecordBatchList(recordBatchList: Array[ArrowRecordBatch]): Unit = {
    recordBatchList.foreach({ recordBatch =>
      if (recordBatch != null)
        recordBatch.close()
    })
  }

  def close(): Unit = {
  }

}

object ColumnarShuffledHashJoin {
  var columnarShuffedHahsJoin: ColumnarShuffledHashJoin = _
  def create(
    resultSchema: StructType,
    joinType: JoinType,
    condition: Option[Expression]): ColumnarShuffledHashJoin = synchronized {
    columnarShuffedHahsJoin = new ColumnarShuffledHashJoin(resultSchema, joinType, condition)
    columnarShuffedHahsJoin
  }
/*
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan
*/

  def close(): Unit = {
    if (columnarShuffedHahsJoin != null) {
      columnarShuffedHahsJoin.close()
    }
  }
}
