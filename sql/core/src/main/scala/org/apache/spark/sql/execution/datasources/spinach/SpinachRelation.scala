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

import java.util.Comparator

import com.google.common.base.Objects
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Descending, Ascending, SortOrder}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.{IndexColumn, InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.datasources.{BaseWriterContainer, PartitionSpec}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter, HadoopFsRelation, HadoopFsRelationProvider, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConversions

class DefaultSource extends HadoopFsRelationProvider with DataSourceRegister {

  override def shortName(): String = "spn"

  override def createRelation(
     sqlContext: SQLContext,
     paths: Array[String],
     dataSchema: Option[StructType],
     partitionColumns: Option[StructType],
     parameters: Map[String, String]): HadoopFsRelation = {
    new SpinachRelation(paths, dataSchema, None, partitionColumns, parameters)(sqlContext)
  }
}

private[spinach] class SpinachRelation(
    override val paths: Array[String],
    maybeDataSchema: Option[StructType],
    maybePartitionSpec: Option[PartitionSpec],
    override val userDefinedPartitionColumns: Option[StructType],
    parameters: Map[String, String])(
    @transient val sqlContext: SQLContext)
  extends HadoopFsRelation(maybePartitionSpec, parameters)
  with Logging {

  private[spinach] def this(
                         paths: Array[String],
                         maybeDataSchema: Option[StructType],
                         maybePartitionSpec: Option[PartitionSpec],
                         parameters: Map[String, String])(
                         sqlContext: SQLContext) = {
    this(
      paths,
      maybeDataSchema,
      maybePartitionSpec,
      maybePartitionSpec.map(_.partitionColumns),
      parameters)(sqlContext)
  }

  // get the meta & data file path.
  private lazy val _metaPaths: Array[FileStatus] = cachedLeafStatuses().filter { status =>
    status.getPath.getName.endsWith(SpinachFileFormat.SPINACH_META_FILE)
  }.toArray

  private lazy val meta: Option[DataSourceMeta] = {
    if (_metaPaths.isEmpty) {
      None
    } else {
      // TODO verify all of the schema from the meta data
      Some(DataSourceMeta.initialize(
        _metaPaths(0).getPath,
        sqlContext.sparkContext.hadoopConfiguration))
    }
  }

  override val dataSchema: StructType = maybeDataSchema.getOrElse(
    meta.map(_.schema)
      .getOrElse(
        throw new IllegalStateException("Cannot get the meta info from file spinach.meta")))

  override def needConversion: Boolean = false

  override def equals(other: Any): Boolean = other match {
    case that: SpinachRelation =>
      paths.toSet == that.paths.toSet &&
        dataSchema == that.dataSchema &&
        schema == that.schema &&
        partitionColumns == that.partitionColumns
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(
      paths.toSet,
      dataSchema,
      schema,
      partitionColumns)
  }

  override def buildInternalScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[InternalRow] = {
    // TODO this probably used by column pruning
    // val output = StructType(requiredColumns.map(dataSchema(_))).toAttributes
    // get the data path from the given paths
    // TODO get the index file from the given paths

    meta match {
      case Some(mt) =>
        SpinachTableScan(mt, this, filters, requiredColumns, inputPaths, broadcastedConf).execute()
      case None =>
        sqlContext.sparkContext.emptyRDD[InternalRow]
    }
  }

  // currently we don't support any filtering.
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    new SpinachOutputWriterFactory
  }

  override def createWriteContainer(df: DataFrame, isAppend: Boolean, job: Job)
  : BaseWriterContainer = {
//    // used by initialize the outputCommitter.
//    job.setOutputFormatClass(classOf[SpinachFileOutputFormat])

    if (partitionColumns.isEmpty) {
      new NonDynamicPartitionWriteContainer(this, job, isAppend, df.schema)
    } else {
      // too many small files generated in the dynamic partition, we don't want to cache that
      throw new UnsupportedOperationException("We don't support dynamic partition yet.")
    }
  }

  def createIndex(indexName: TableIdentifier, indexColumns: Array[IndexColumn]): Unit = {
    // TODO Also write into Spinach general meta
    if (paths.isEmpty) {
      // the input path probably be pruned, do nothing
    } else {
      val data = paths.filter(_.endsWith(SpinachFileFormat.SPINACH_DATA_EXTENSION))
      val ids = indexColumns.map(c => schema.map(_.name).toIndexedSeq.indexOf(c.columnName))
      val keySchema = StructType(ids.map(schema.toIndexedSeq(_)))
      assert(!ids.exists(id => id < 0), "Index column not exists in schema.")
      val ordering = buildOrdering(ids)
      val trees = data.foreach(d => {
        // scan every data file
        val file = new Path(d)
        val reader = new SpinachDataReader2(file, schema, ids)
        // TODO maybe use Long?
        // TODO use KeyGenerator like HashSemiJoin
        val hashMap = new java.util.HashMap[InternalRow, java.util.ArrayList[Int]]()
        var cnt = 0
        while (reader.nextKeyValue()) {
          val v = reader.getCurrentValue
          if (!hashMap.containsKey(v)) {
            val list = new java.util.ArrayList[Int]()
            list.add(cnt)
            hashMap.put(v, list)
          } else {
            hashMap.get(v).add(cnt)
          }
          cnt = cnt + 1
        }
        reader.close()
        val partitionUniqueSize = hashMap.size()
        val uniqueKeys = hashMap.keySet().toArray.asInstanceOf[Array[InternalRow]]
        lazy val comparator: Comparator[InternalRow] = new Comparator[InternalRow]() {
          override def compare(o1: InternalRow, o2: InternalRow): Int = {
            if (o1 == null && o2 == null) {
              0
            } else if (o1 == null) {
              -1
            } else if (o2 == null) {
              1
            } else {
              ordering.compare(o1, o2)
            }
          }
        }
        // sort keys
        java.util.Arrays.sort(uniqueKeys, comparator)
        // build index file
        val indexFile = new Path(d.replace(
          SpinachFileFormat.SPINACH_DATA_EXTENSION, SpinachFileFormat.SPINACH_INDEX_EXTENSION))
        val fs: FileSystem = indexFile.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
        val fileOut: FSDataOutputStream = fs.create(indexFile, false)
        var i = 0
        var fileOffset = 0
        val offsetMap = new java.util.HashMap[InternalRow, Int]()
        while (i < partitionUniqueSize) {
          offsetMap.put(uniqueKeys(i), fileOffset)
          val byteDataFromKey = internalRowToByte(uniqueKeys(i), keySchema)
          fileOut.write(byteDataFromKey)
          fileOffset = fileOffset + byteDataFromKey.size
          val rowIds = hashMap.get(uniqueKeys(i))
          // write size
          fileOut.write(intToBytes(rowIds.size()))
          fileOffset = fileOffset + 4
          var idIter = 0
          while (idIter < rowIds.size()) {
            fileOut.write(intToBytes(rowIds.get(idIter)))
            fileOffset = fileOffset + 4
            idIter = idIter + 1
          }
          i = i + 1
        }
        val treeShape = BTreeUtils.generate2(partitionUniqueSize)
        val uniqueKeysList = new java.util.LinkedList[InternalRow]()
        uniqueKeysList.addAll(JavaConversions.asJavaList(uniqueKeys))
        writeTreeToOut(treeShape, fileOut, offsetMap, fileOffset, uniqueKeysList, keySchema, 0)
        assert(uniqueKeysList.size == 1)
        // write index meta
        fileOut.write(intToBytes(offsetMap.get(uniqueKeysList.getFirst)))
        fileOut.close()
      })
    }

    def buildOrdering(requiredIds: Array[Int]): Ordering[InternalRow] = {
      val order = requiredIds.toSeq.map(id => SortOrder(
        BoundReference(id, schema(id).dataType, nullable = true),
        if (indexColumns(requiredIds.indexOf(id)).isAscending) Ascending else Descending))
      GenerateOrdering.generate(order, schema.toAttributes)
    }

    def internalRowToByte(row: InternalRow, schema: StructType): Array[Byte] = {
      var idx = 0
      var offset = 0
      val types = schema.map(_.dataType)
      val buffer = new Array[Byte](schema.defaultSize)
      while (idx < row.numFields) {
        val byteData = types(idx) match {
          case IntegerType => Platform.putInt(buffer, offset, row.getInt(idx))
          // TODO
          case _ => sys.error("Not implemented yet!")
        }
        idx = idx + 1
        offset = offset + types(idx).defaultSize
      }
      buffer
    }

    def intToBytes(id: Int): Array[Byte] = {
      val buffer = new Array[Byte](4)
      Platform.putInt(buffer, 0, id)
      buffer
    }

    def writeTreeToOut(
                        tree: BTreeNode,
                        out: FSDataOutputStream,
                        map: java.util.HashMap[InternalRow, Int],
                        fileOffset: Int,
                        keysList: java.util.LinkedList[InternalRow],
                        keySchema: StructType,
                        listOffset: Int): Int = {
      if (tree.children.isEmpty) {
        var subOffset = 0
        val keyVal = keysList.getFirst
        val byteDataFromKey = internalRowToByte(keyVal, keySchema)
        out.write(byteDataFromKey)
        subOffset = subOffset + byteDataFromKey.size
        out.write(intToBytes(map.get(keyVal)))
        subOffset = subOffset + 4
        var rmCount = 1
        while (rmCount < tree.root) {
          keysList.remove(listOffset + 1)
          rmCount = rmCount + 1
        }
        map.put(keyVal, fileOffset)
        subOffset
      } else {
        val childrenCount = tree.children.size
        var iter = 0
        var subOffset = 0
        while (iter < childrenCount) {
          val subTree = tree.children(iter)
          subOffset = subOffset + writeTreeToOut(
            subTree, out, map, fileOffset + subOffset, keysList, keySchema, listOffset + iter)
          iter = iter + 1
        }
        subOffset
      }
    }
  }
}

private[spinach] class SpinachOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext) extends OutputWriter {
  private val writer = new FileOutputFormat[NullWritable, InternalRow] {
    override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
      new Path(path, getFileName(extension))
    }

    override def getRecordWriter(context: TaskAttemptContext)
    : RecordWriter[NullWritable, InternalRow] = {
      val conf: Configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
      val isCompressed: Boolean = FileOutputFormat.getCompressOutput(context)

      val file: Path = getDefaultWorkFile(context, SpinachFileFormat.SPINACH_DATA_EXTENSION)
      val fs: FileSystem = file.getFileSystem(conf)
      val fileOut: FSDataOutputStream = fs.create(file, false)
      new SpinachDataWriter2(isCompressed, fileOut, dataSchema)
    }
  }.getRecordWriter(context)

  override def write(row: Row): Unit = throw new NotImplementedError("write(row: Row)")
  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    writer.write(NullWritable.get(), row)
  }
  override def close(): Unit = writer.close(context)

  def getFileName(extension: String): String = {
    val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
    // this is the way how we pass down the uuid
    val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId = SparkHadoopUtil.get.getTaskAttemptIDFromTaskAttemptContext(context)
    val split = taskAttemptId.getTaskID.getId
    f"part-r-$split%05d-${uniqueWriteJobId}$extension"
  }

  def getFileName(): String = getFileName(SpinachFileFormat.SPINACH_DATA_EXTENSION)
}

private[spinach] class SpinachOutputWriterFactory extends OutputWriterFactory {
  override def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    // TODO possible to do some setup on executor side initialization
    new SpinachOutputWriter(path, dataSchema, context)
  }
}

private[spinach] case class SpinachTableScan(
    meta: DataSourceMeta,
    @transient relation: SpinachRelation,
    filters: Array[Filter],
    requiredColumns: Array[String],
    @transient inputPaths: Array[FileStatus],
    bc: Broadcast[SerializableConfiguration])
  extends Logging {
  @transient private val sqlContext = relation.sqlContext

  // TODO serialize / deserialize the schema via SpinachMeta instead
  def execute(): RDD[InternalRow] = {
    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)

    // TODO Tries to push down filters if spinach filter push-down is enabled
    if (inputPaths.isEmpty) {
      // the input path probably be pruned, return an empty RDD.
      return sqlContext.sparkContext.emptyRDD[InternalRow]
    }
    // TODO write our own RDD, so we can pass down the info via closure instead

    FileInputFormat.setInputPaths(job, inputPaths.map(_.getPath): _*)
    conf.set(SpinachFileFormat.SPINACH_META_SCHEMA, meta.schema.json)

    SpinachFileFormat.setRequiredColumnIds(conf, meta.schema, requiredColumns)

    sqlContext.sparkContext.newAPIHadoopRDD(
      conf, classOf[SpinachFileInputFormat], classOf[NullWritable], classOf[InternalRow]).map(_._2)
  }
}
