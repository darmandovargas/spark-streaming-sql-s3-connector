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
package com.amazonaws.spark.sql.streaming.connector

import java.io.{FileNotFoundException, InputStream}
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.util.LineReader

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructType}

// Opens each key directly: Spark listStatus costs S3A an extra LIST and HEAD per file.
class S3ConnectorTextRelation(
    override val sqlContext: SQLContext,
    paths: Seq[String],
    override val schema: StructType,
    hadoopConf: Configuration,
    filesPerTask: Int,
    ignoreMissingFiles: Boolean,
    minReadTasks: Int = 1) extends BaseRelation with TableScan with Serializable {

  override def buildScan(): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val confEntries = sc.broadcast(
      hadoopConf.iterator().asScala.map(e => e.getKey -> e.getValue).toArray)
    val ignoreMissing = ignoreMissingFiles
    val numTasks = S3ConnectorTextRelation.numReadTasks(paths.length, filesPerTask, minReadTasks)

    sc.parallelize(paths, numTasks).mapPartitions { pathIter =>
      val conf = new Configuration(false)
      confEntries.value.foreach { case (k, v) => conf.set(k, v) }
      val codecs = new CompressionCodecFactory(conf)
      pathIter.flatMap { p =>
        S3ConnectorTextRelation.readLines(new Path(p), conf, codecs, ignoreMissing)
      }
    }
  }
}

object S3ConnectorTextRelation {

  // filesPerTask files per task, but at least minTasks tasks (never more tasks than files) so a small
  // batch still spreads over the cores. Batches of minTasks * filesPerTask files or more are unchanged.
  def numReadTasks(numFiles: Int, filesPerTask: Int, minTasks: Int): Int = {
    if (numFiles <= 0) {
      1
    } else {
      val bySize = math.ceil(numFiles.toDouble / math.max(1, filesPerTask)).toInt
      math.min(numFiles, math.max(bySize, math.max(1, minTasks)))
    }
  }

  private val UnsupportedOptions = Set(
    "wholetext", "linesep", "encoding", "charset",
    "recursivefilelookup", "modifiedbefore", "modifiedafter")

  def supports(
      fileFormat: String,
      schema: StructType,
      partitionColumns: Seq[String],
      options: Map[String, String]): Boolean = {
    fileFormat.equalsIgnoreCase("text") &&
      partitionColumns.isEmpty &&
      schema.fields.length == 1 &&
      schema.fields.head.dataType == StringType &&
      !options.keys.exists(k => UnsupportedOptions.contains(k.toLowerCase))
  }

  private[connector] def readLines(
      path: Path,
      conf: Configuration,
      codecs: CompressionCodecFactory,
      ignoreMissing: Boolean): Iterator[Row] = {
    val fs = path.getFileSystem(conf)
    val raw: InputStream =
      try {
        fs.open(path)
      } catch {
        case _: FileNotFoundException if ignoreMissing => return Iterator.empty
      }
    val codec = codecs.getCodec(path)
    val in = if (codec == null) raw else codec.createInputStream(raw)
    val reader = new LineReader(in, conf)
    val text = new Text()
    var closed = false

    def close(): Unit = if (!closed) {
      closed = true
      reader.close()
    }

    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

    new Iterator[Row] {
      private var pending: Row = advance()

      private def advance(): Row = {
        if (closed) {
          null
        } else if (reader.readLine(text, Int.MaxValue, Int.MaxValue) == 0) {
          close()
          null
        } else {
          Row(new String(text.getBytes, 0, text.getLength, StandardCharsets.UTF_8))
        }
      }

      override def hasNext: Boolean = pending != null

      override def next(): Row = {
        val row = pending
        pending = advance()
        row
      }
    }
  }
}
