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

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.{MapData, ArrayData}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{Decimal, DataType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.{InternalRow, IndexColumn, TableIdentifier}
import org.apache.spark.sql.execution.{RowIterator, SparkPlan, RunnableCommand, UnaryNode}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Creates an index for table on indexColumns
 */
case class CreateIndex(
    indexName: String,
    tableName: TableIdentifier,
    indexColumns: Array[IndexColumn],
    ifNotExists: Boolean) extends RunnableCommand with Logging {
  override def children: Seq[LogicalPlan] = Seq.empty

  override val output: Seq[Attribute] = Seq.empty

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.catalog
    assert(catalog.tableExists(tableName), s"$tableName not exists")
    val indexIdent = TableIdentifier(indexName)
    if (catalog.tableExists(indexIdent)) {
      val msg = s"already exists a index named $indexName"
      if (ifNotExists) {
        // do nothing
        logWarning(msg)
      } else {
        sys.error(msg)
      }
    } else {
      // TODO See [[[IndexMarker]]]
      catalog.registerTable(indexIdent, IndexMarker())
      catalog.lookupRelation(tableName) match {
        case Subquery(_, LogicalRelation(r: SpinachRelation, _)) =>
          r.createIndex(indexIdent, indexColumns)
        case _ => sys.error("Only support CreateIndex for SpinachRelation")
      }
    }
    Seq.empty
  }
}

/**
 * Drops an index
 */
case class DropIndex(
    indexIdent: String,
    ifExists: Boolean) extends RunnableCommand {

  override def children: Seq[LogicalPlan] = Seq.empty

  override val output: Seq[Attribute] = Seq.empty

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.catalog
    if (!catalog.tableExists(TableIdentifier(indexIdent))) {
      if (ifExists) {
        // do nothing
      } else {
        sys.error(s"$indexIdent not found")
      }
    } else {
      sqlContext.catalog.unregisterTable(TableIdentifier(indexIdent))
    }
    // TODO delete index file and delete index meta from spinach meta
    Seq.empty
  }
}

/**
 * use as the marker to distinguish between table and index in catalog.
 * TODO should use a separate place and APIs
 */
case class IndexMarker() extends LeafNode {
  override val output: Seq[Attribute] = Seq.empty
}

private[spinach] trait IndexValueBucket {
  def length: Int
  def apply(idx: Int): Int
}

private[spinach] abstract class IndexGuideNode(length: Int) extends IndexNode {
  override def keyAt(idx: Int): InternalRow
  def childAt(idx: Int): IndexNode
  override def isLeaf: Boolean = false
}

private[spinach] abstract class IndexLeafNode(length: Int) extends IndexNode {
  override def keyAt(idx: Int): InternalRow
  def valueAt(idx: Int): IndexValueBucket
  def next: IndexLeafNode
  override def isLeaf: Boolean = true
}

private[spinach] trait IndexNode {
  def length: Int
  def keyAt(idx: Int): InternalRow
  def isLeaf: Boolean
}
