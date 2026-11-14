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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileNotFoundException, InputStream, SequenceInputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{Callable, ExecutionException, Executors, Future, Semaphore, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataInputStream, Path}
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
    minReadTasks: Int = 1,
    readConcurrency: Int = 1,
    maxBufferedFileBytes: Long = S3ConnectorTextRelation.DefaultMaxBufferedFileBytes)
  extends BaseRelation with TableScan with Serializable {

  override def buildScan(): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val confEntries = sc.broadcast(
      hadoopConf.iterator().asScala.map(e => e.getKey -> e.getValue).toArray)
    val ignoreMissing = ignoreMissingFiles
    val concurrency = readConcurrency
    val maxBuffered = maxBufferedFileBytes
    val numTasks = S3ConnectorTextRelation.numReadTasks(paths.length, filesPerTask, minReadTasks)

    sc.parallelize(paths, numTasks).mapPartitions { pathIter =>
      val conf = new Configuration(false)
      confEntries.value.foreach { case (k, v) => conf.set(k, v) }
      val codecs = new CompressionCodecFactory(conf)
      if (concurrency <= 1) {
        pathIter.flatMap { target =>
          val (p, known) = S3ConnectorTextRelation.parseTarget(target)
          S3ConnectorTextRelation.readLines(new Path(p), conf, codecs, ignoreMissing, known)
        }
      } else {
        S3ConnectorTextRelation.readConcurrently(pathIter, conf, codecs, ignoreMissing, concurrency, maxBuffered)
      }
    }
  }
}

object S3ConnectorTextRelation {

  // A read target is the path, optionally followed by the size and the eTag of the object taken from the S3 event,
  // separated by \u0001. With a known size S3A does not send a HEAD request before reading the file.
  private val InfoSeparator = '\u0001'

  def withKnownInfo(path: String, sizeBytes: Long, eTag: String): String =
    path + InfoSeparator + sizeBytes + InfoSeparator + eTag

  private[connector] def parseTarget(target: String): (String, Option[(Long, String)]) = {
    if (target.indexOf(InfoSeparator) < 0) {
      (target, None)
    } else {
      // limit -1 keeps a trailing empty eTag as a part (the default split would drop it and lose the known size)
      val parts = target.split(InfoSeparator.toString, -1)
      val known =
        if (parts.length == 3) scala.util.Try(parts(1).toLong).toOption.filter(_ >= 0).map(s => (s, parts(2))) else None
      (parts(0), known)
    }
  }

  // Opens the file with a file status built from the event size so S3A skips its HEAD request. The eTag of the
  // event is deliberately not given to S3A: a different eTag format could make its change detection fail the read.
  // Any problem (Hadoop without openFile, other file system, unexpected class) falls back to the normal open.
  private[connector] def openKnown(fs: FileSystem, path: Path, known: Option[(Long, String)]): FSDataInputStream = {
    known match {
      case Some((size, _)) =>
        try {
          openWithStatus(fs, path, size)
        } catch {
          case fnf: FileNotFoundException => throw fnf
          case NonFatal(_) => fs.open(path)
        }
      case None => fs.open(path)
    }
  }

  private def openWithStatus(fs: FileSystem, path: Path, size: Long): FSDataInputStream = {
    val qualified = fs.makeQualified(path)
    val statusClass = Class.forName("org.apache.hadoop.fs.s3a.S3AFileStatus")
    val ctor = statusClass.getConstructor(classOf[Long], classOf[Long], classOf[Path], classOf[Long],
      classOf[String], classOf[String], classOf[String])
    val status = ctor.newInstance(Long.box(size), Long.box(0L), qualified, Long.box(1L), "", null, null)
      .asInstanceOf[FileStatus]
    val builderClass = Class.forName("org.apache.hadoop.fs.FutureDataInputStreamBuilder")
    val builder = fs.getClass.getMethod("openFile", classOf[Path]).invoke(fs, qualified)
    val withStatus = builderClass.getMethod("withFileStatus", classOf[FileStatus]).invoke(builder, status)
    val future = builderClass.getMethod("build").invoke(withStatus)
      .asInstanceOf[java.util.concurrent.CompletableFuture[FSDataInputStream]]
    try {
      future.get()
    } catch {
      case e: ExecutionException if e.getCause.isInstanceOf[FileNotFoundException] =>
        throw e.getCause
    }
  }

  val DefaultReadConcurrency: Int = 8
  val DefaultMaxBufferedFileBytes: Long = 8L * 1024 * 1024

  // Default minimum number of read tasks: 4 tasks per core, so a small batch never ends in a half empty
  // second wave of tasks (3000 files on 8 cores is 32 tasks, not 12 tasks on 8 cores).
  def defaultMinReadTasks(defaultParallelism: Int): Int = 4 * math.max(1, defaultParallelism)

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
      ignoreMissing: Boolean,
      known: Option[(Long, String)] = None): Iterator[Row] = {
    val fs = path.getFileSystem(conf)
    val raw: InputStream =
      try {
        openKnown(fs, path, known)
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

  // Reads up to `concurrency` files at once inside one task and hands the rows back in submission order,
  // so the output is the same as reading the files one after another. At most `concurrency` files are
  // buffered (or being consumed) at any moment, each up to `maxBuffered` bytes in memory.
  private[connector] def readConcurrently(
      pathIter: Iterator[String],
      conf: Configuration,
      codecs: CompressionCodecFactory,
      ignoreMissing: Boolean,
      concurrency: Int,
      maxBuffered: Long): ConcurrentFileReader =
    new ConcurrentFileReader(pathIter, conf, codecs, ignoreMissing, math.max(1, concurrency), math.max(1L, maxBuffered))

  private val readerThreadIds = new AtomicInteger(0)

  // `bytes` is null for a skipped (missing) file; `rest` is the unread tail of a file bigger than the buffer.
  private final case class Fetched(path: Path, bytes: Array[Byte], rest: InputStream)

  private[connector] final class ConcurrentFileReader(
      pathIter: Iterator[String],
      conf: Configuration,
      codecs: CompressionCodecFactory,
      ignoreMissing: Boolean,
      concurrency: Int,
      maxBuffered: Long) extends Iterator[Row] with AutoCloseable {

    @volatile private var closed = false
    private val permits = new Semaphore(concurrency)
    private val queue = new java.util.ArrayDeque[Future[Fetched]]()
    private val poolId = readerThreadIds.incrementAndGet()
    private val threadNo = new AtomicInteger(0)
    private val pool = Executors.newFixedThreadPool(concurrency, new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, s"s3conn-read-$poolId-${threadNo.incrementAndGet()}")
        t.setDaemon(true)
        t
      }
    })

    private var current: LineIterator = _
    private var currentHeldPermit = false

    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))
    fill()

    private def fill(): Unit = {
      while (!closed && pathIter.hasNext && permits.tryAcquire()) {
        val p = pathIter.next()
        queue.add(pool.submit(new Callable[Fetched] {
          override def call(): Fetched = fetch(p)
        }))
      }
    }

    private def closeQuietly(c: AutoCloseable): Unit =
      if (c != null) try c.close() catch { case _: Throwable => }

    private def fetch(target: String): Fetched = {
      val (p, known) = parseTarget(target)
      val path = new Path(p)
      val fs = path.getFileSystem(conf)
      val in: InputStream =
        try {
          openKnown(fs, path, known)
        } catch {
          case _: FileNotFoundException if ignoreMissing => return Fetched(path, null, null)
        }
      try {
        val limit = maxBuffered + 1
        val out = new ByteArrayOutputStream(math.min(limit, 64L * 1024).toInt)
        val chunk = new Array[Byte](32 * 1024)
        var total = 0L
        var eof = false
        while (!eof && total < limit) {
          val n = in.read(chunk, 0, math.min(chunk.length.toLong, limit - total).toInt)
          if (n < 0) eof = true else { out.write(chunk, 0, n); total += n }
        }
        if (closed) {
          closeQuietly(in)
          Fetched(path, null, null)
        } else if (eof) {
          closeQuietly(in)
          known.foreach { case (size, _) =>
            if (size != total) {
              throw new java.io.IOException(
                s"File $path has $total bytes but the S3 event said $size: refusing to use the event size")
            }
          }
          Fetched(path, out.toByteArray, null)
        } else {
          Fetched(path, out.toByteArray, in)
        }
      } catch {
        case t: Throwable =>
          closeQuietly(in)
          throw t
      }
    }

    private def unwrap(e: ExecutionException): Throwable = if (e.getCause != null) e.getCause else e

    private def releaseCurrent(): Unit = {
      if (current != null) {
        current.close()
        current = null
      }
      if (currentHeldPermit) {
        currentHeldPermit = false
        permits.release()
      }
    }

    private def advanceFile(): Boolean = {
      releaseCurrent()
      fill()
      if (queue.isEmpty) {
        false
      } else {
        val fetchedFuture = queue.poll()
        val fetched =
          try {
            fetchedFuture.get()
          } catch {
            case e: ExecutionException =>
              permits.release()
              throw unwrap(e)
          }
        if (fetched.bytes == null) {
          permits.release()
          fill()
        } else {
          currentHeldPermit = true
          current = new LineIterator(fetched)
        }
        true
      }
    }

    override def hasNext: Boolean = {
      if (closed) {
        false
      } else {
        try {
          while (current == null || !current.hasNext) {
            if (!advanceFile()) {
              close()
              return false
            }
          }
          true
        } catch {
          case t: Throwable =>
            close()
            throw t
        }
      }
    }

    override def next(): Row = {
      if (!hasNext) throw new NoSuchElementException("no more rows")
      current.next()
    }

    override def close(): Unit = {
      if (!closed) {
        closed = true
        try {
          releaseCurrent()
        } catch {
          case _: Throwable =>
        }
        while (!queue.isEmpty) {
          val f = queue.poll()
          if (!f.cancel(true) && f.isDone && !f.isCancelled) {
            try closeQuietly(f.get().rest) catch { case _: Throwable => }
          }
        }
        pool.shutdownNow()
        try pool.awaitTermination(5, TimeUnit.SECONDS) catch { case _: InterruptedException => }
      }
    }

    // Lines of one buffered file, same decoding as the sequential reader.
    private final class LineIterator(fetched: Fetched) extends Iterator[Row] {
      private val raw: InputStream = {
        val head = new ByteArrayInputStream(fetched.bytes)
        if (fetched.rest == null) head else new SequenceInputStream(head, fetched.rest)
      }
      private val codec = codecs.getCodec(fetched.path)
      private val in: InputStream = if (codec == null) raw else codec.createInputStream(raw)
      private val reader = new LineReader(in, conf)
      private val text = new Text()
      private var done = false
      private var pending: Row = advance()

      private def advance(): Row = {
        if (done) {
          null
        } else if (reader.readLine(text, Int.MaxValue, Int.MaxValue) == 0) {
          close()
          null
        } else {
          Row(new String(text.getBytes, 0, text.getLength, StandardCharsets.UTF_8))
        }
      }

      def close(): Unit = if (!done) {
        done = true
        reader.close()
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
