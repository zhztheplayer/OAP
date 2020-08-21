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

package com.intel.oap.expression

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

class ColumnarWindow(
    partIndex: Int,
    groupingExpressions: Seq[NamedExpression],
    originalInputAttributes: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    output: Seq[Attribute],
    numInputBatches: SQLMetric,
    numOutputBatches: SQLMetric,
    numOutputRows: SQLMetric,
    aggrTime: SQLMetric,
    elapseTime: SQLMetric,
    sparkConf: SparkConf) extends ColumnarAggregation(partIndex, groupingExpressions,
  originalInputAttributes, aggregateExpressions, aggregateAttributes, resultExpressions,
  output, numInputBatches, numOutputBatches, numOutputRows, aggrTime, elapseTime, sparkConf) {
  override def createIterator(cbIterator: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {

    super.createIterator(cbIterator)
  }
}
