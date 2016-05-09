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

import org.apache.hadoop.fs.{Path, FSDataOutputStream}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

private[spinach] class SpinachIndexDataWriter(
    isCompressed: Boolean,
    out: FSDataOutputStream) extends RecordWriter[NullWritable, InternalRow] {

  override def write(ignore: NullWritable, row: InternalRow): Unit = {
    var idx = 0
  }

  override def close(context: TaskAttemptContext): Unit = {
  }
}

private[spinach] class SpinachIndexDataReader(
    path: Path,
    schema: StructType,
    requiredIds: Array[Int]) extends RecordReader[NullWritable, InternalRow] {
  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
  }

  override def getProgress: Float = {
    1.0f
  }

  override def nextKeyValue(): Boolean = true

  def getCurrentValue: InternalRow = InternalRow.empty

  override def getCurrentKey: NullWritable = NullWritable.get()

  override def close(): Unit = {
  }
}