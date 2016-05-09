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
 * Another way to create index (incomplete)
 */
case class CreateBTreeIndex(
    indexIdent: String,
    tableName: String,
    indexKey: Seq[SortOrder],
    ifNotExists: Boolean) extends UnaryNode {

  lazy val keyRow = new IndexKey
  lazy val keyOrdering = newOrdering(indexKey, Seq.empty)
  override def child: SparkPlan =
    sqlContext.planner.plan(sqlContext.table(tableName).logicalPlan).next()
  override val output: Seq[Attribute] = Seq.empty

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(indexKey)

  protected override def doExecute(): RDD[InternalRow] = {
    val catalog = sqlContext.catalog
    catalog.registerTable(TableIdentifier(indexIdent), IndexMarker())
    // TODO Also write into Spinach general meta
    child.execute().mapPartitions { iter =>
      // TODO write tree file for every partition
      BTreeUtils.generate(100)
      new RowIterator {
        override def getRow: InternalRow = null
        override def advanceNext(): Boolean = false
      }.toScala
    }
  }
}

/**
 * use as the marker to distinguish between table and index in catalog.
 * TODO should use a separate place and APIs
 */
case class IndexMarker() extends LeafNode {
  override val output: Seq[Attribute] = Seq.empty
}

class IndexKey extends InternalRow {
  // NOTE: all elements in rows only has numElements of 1
  private[this] var rows: Seq[InternalRow] = _
  override def anyNull: Boolean = rows.exists(_.anyNull)

  def this(rs: Seq[InternalRow]) = {
    this()
    rows = rs
  }

  def apply(rs: Seq[InternalRow]): IndexKey = {
    rows = rs
    this
  }

  override def numFields: Int = rows.size

  override def getUTF8String(i: Int): UTF8String =
    rows(i).getUTF8String(0)

  override def get(i: Int, dt: DataType): AnyRef =
    rows(i).get(0, dt)

  override def isNullAt(i: Int): Boolean =
    rows(i).isNullAt(0)

  override def getBoolean(i: Int): Boolean =
    rows(i).getBoolean(0)

  override def getByte(i: Int): Byte =
    rows(i).getByte(0)

  override def getShort(i: Int): Short =
    rows(i).getShort(0)

  override def getInt(i: Int): Int =
    rows(i).getInt(0)

  override def getLong(i: Int): Long =
    rows(i).getLong(0)

  override def getFloat(i: Int): Float =
    rows(i).getFloat(0)

  override def getDouble(i: Int): Double =
    rows(i).getDouble(0)

  override def getDecimal(i: Int, precision: Int, scale: Int): Decimal = {
    rows(i).getDecimal(0, precision, scale)
  }

  override def getBinary(i: Int): Array[Byte] =
    rows(i).getBinary(0)

  override def getArray(i: Int): ArrayData =
    rows(i).getArray(0)

  override def getInterval(i: Int): CalendarInterval =
    rows(i).getInterval(0)

  override def getMap(i: Int): MapData =
    rows(i).getMap(0)

  override def getStruct(i: Int, numFields: Int): InternalRow =
    rows(i).getStruct(0, numFields)

  override def copy(): InternalRow = {
    val copies = rows.map(_.copy())
    new IndexKey(copies)
  }

  override def toString: String = {
    rows.map(_.toString).reduce(_ + _)
  }
}
