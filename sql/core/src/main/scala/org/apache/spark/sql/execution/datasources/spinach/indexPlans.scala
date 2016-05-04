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

package org.apache.spark.sql.execution.datasources.spinach

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{SortOrder, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Command, LogicalPlan}
import org.apache.spark.sql.catalyst.{InternalRow, IndexColumn, TableIdentifier}
import org.apache.spark.sql.execution.{RowIterator, SparkPlan, RunnableCommand, UnaryNode}
import org.apache.spark.sql.execution.datasources.LogicalRelation


/**
 * Creates an index for table on indexColumns
 */
case class CreateIndex(
    indexIdent: String,
    tableName: TableIdentifier,
    indexColumns: Array[IndexColumn],
    ifNotExists: Boolean) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override val output: Seq[Attribute] = Seq.empty
}

/**
 * Drops an index
 */
case class DropIndex(
    indexIdent: String,
    ifNotExists: Boolean) extends RunnableCommand {

  override def children: Seq[LogicalPlan] = Seq.empty

  override val output: Seq[Attribute] = Seq.empty

  override def run(sqlContext: SQLContext): Seq[Row] = {
    // TODO delete index file and meta
    // TODO support `ifNotExists`
    sqlContext.catalog.unregisterTable(TableIdentifier(indexIdent))
    Seq.empty
  }
}

case class CreateBTreeIndex(
    indexIdent: String,
    child: SparkPlan,
    indexKey: Seq[SortOrder],
    ifNotExists: Boolean) extends UnaryNode {

  override val output: Seq[Attribute] = Seq.empty

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(indexKey)

  protected override def doExecute(): RDD[InternalRow] = {
    val catalog = sqlContext.catalog
    catalog.registerTable(TableIdentifier(indexIdent), IndexMarker())
    child.execute().mapPartitions { iter =>
      new RowIterator {
        /**
         * Retrieve the row from this iterator. This method is idempotent. It is illegal to call this
         * method after [[advanceNext()]] has returned `false`.
         */
        override def getRow: InternalRow = ???

        /**
         * Advance this iterator by a single row. Returns `false` if this iterator has no more rows
         * and `true` otherwise. If this returns `true`, then the new row can be retrieved by calling
         * [[getRow]].
         */
        override def advanceNext(): Boolean = ???
      }.toScala
    }
    sparkContext.parallelize(Nil)
  }
}

/**
 * use as the marker to distinguish between table and index in catalog.
 * TODO should use a separate place and APIs
 */
case class IndexMarker() extends LeafNode {
  override val output: Seq[Attribute] = Seq.empty
}
