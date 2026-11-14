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

import java.io.{File, FileOutputStream, IOException}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPOutputStream

import scala.collection.JavaConverters._

import com.amazonaws.spark.sql.streaming.connector.TestUtils.withTestTempDir
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.streaming.connector.s3.S3SparkUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

// Local file system whose open() takes a configurable time, records which threads open files and how many
// opens overlap, so the tests can see the concurrency of the reader.
class SlowFileSystem extends RawLocalFileSystem {
  override def getScheme: String = "slowfs"
  override def getUri: URI = URI.create("slowfs:///")

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    SlowFileSystem.openThreads.add(Thread.currentThread().getName)
    val now = SlowFileSystem.inFlight.incrementAndGet()
    SlowFileSystem.maxInFlight.updateAndGet(new java.util.function.IntUnaryOperator {
      override def applyAsInt(prev: Int): Int = math.max(prev, now)
    })
    try {
      Thread.sleep(SlowFileSystem.delayMs(f.getName))
    } finally {
      SlowFileSystem.inFlight.decrementAndGet()
    }
    super.open(f, bufferSize)
  }
}

object SlowFileSystem {
  val openThreads: java.util.Set[String] = ConcurrentHashMap.newKeySet[String]()
  val inFlight = new AtomicInteger(0)
  val maxInFlight = new AtomicInteger(0)
  @volatile var delayMs: (String => Long) = (_: String) => 0L

  def reset(delay: String => Long): Unit = {
    openThreads.clear()
    inFlight.set(0)
    maxInFlight.set(0)
    delayMs = delay
  }
}

class FailingFileSystem extends RawLocalFileSystem {
  override def getScheme: String = "failfs"
  override def getUri: URI = URI.create("failfs:///")

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    if (f.getName.startsWith("bad")) throw new IOException(s"boom ${f.getName}")
    Thread.sleep(5)
    super.open(f, bufferSize)
  }
}

class S3ConnectorConcurrentReadSuite extends S3ConnectorTestBase with SharedSparkSession {

  private val textSchema = StructType(Seq(StructField("log", StringType, nullable = false)))

  private def write(dir: File, name: String, content: String): String = {
    val file = new File(dir, name)
    Files.write(file.toPath, content.getBytes(StandardCharsets.UTF_8))
    file.getAbsolutePath
  }

  private def writeGzip(dir: File, name: String, content: String): String = {
    val file = new File(dir, name)
    val out = new GZIPOutputStream(new FileOutputStream(file))
    try out.write(content.getBytes(StandardCharsets.UTF_8)) finally out.close()
    file.getAbsolutePath
  }

  private def hadoopConf: Configuration = {
    val conf = spark.sessionState.newHadoopConf()
    conf.set("fs.slowfs.impl", classOf[SlowFileSystem].getName)
    conf.set("fs.failfs.impl", classOf[FailingFileSystem].getName)
    conf
  }

  private def registerFileSystems(): Unit = {
    spark.sparkContext.hadoopConfiguration.set("fs.slowfs.impl", classOf[SlowFileSystem].getName)
    spark.sparkContext.hadoopConfiguration.set("fs.failfs.impl", classOf[FailingFileSystem].getName)
  }

  // One partition, so the collected rows keep the order in which the files are read.
  private def readAll(
      paths: Seq[String],
      concurrency: Int,
      maxBuffered: Long = S3ConnectorTextRelation.DefaultMaxBufferedFileBytes,
      ignoreMissing: Boolean = false): Seq[String] = {
    registerFileSystems()
    val relation = new S3ConnectorTextRelation(spark.sqlContext, paths, textSchema, hadoopConf,
      filesPerTask = 100000, ignoreMissingFiles = ignoreMissing, minReadTasks = 1,
      readConcurrency = concurrency, maxBufferedFileBytes = maxBuffered)
    S3SparkUtils.ofRows(spark, LogicalRelation(relation, isStreaming = false))
      .collect().map(_.getString(0)).toSeq
  }

  private def viaDataSource(paths: Seq[String]): DataFrame = {
    val relation = DataSource(spark, paths = paths, userSpecifiedSchema = Some(textSchema),
      className = "text", options = Map.empty).resolveRelation(checkFilesExist = false)
    S3SparkUtils.ofRows(spark, LogicalRelation(relation, isStreaming = false))
  }

  private def readerThreads: Int =
    Thread.getAllStackTraces.keySet.asScala.count(t => t.getName.startsWith("s3conn-read") && t.isAlive)

  private def waitNoReaderThreads(): Unit = {
    val deadline = System.currentTimeMillis + 10000
    while (readerThreads > 0 && System.currentTimeMillis < deadline) Thread.sleep(25)
    readerThreads shouldBe 0
  }

  private def direct(paths: Seq[String], concurrency: Int, ignoreMissing: Boolean = false,
      maxBuffered: Long = S3ConnectorTextRelation.DefaultMaxBufferedFileBytes) = {
    registerFileSystems()
    val conf = hadoopConf
    S3ConnectorTextRelation.readConcurrently(paths.iterator, conf, new CompressionCodecFactory(conf),
      ignoreMissing, concurrency, maxBuffered)
  }

  test("same rows in the same order for readConcurrency 1, 4 and 16, around the buffer limit") {
    withTestTempDir { dir =>
      val limit = 1024
      val paths = Seq(
        write(dir, "empty", ""),
        write(dir, "no-newline", "a b c"),
        write(dir, "trailing", "x\ny\n"),
        write(dir, "blank-lines", "\n\nz\n\n"),
        write(dir, "crlf", "p\r\nq\r\n"),
        write(dir, "lone-cr", "m\rn\ro"),
        write(dir, "unicode", "café 中文 🚀\nlast"),
        writeGzip(dir, "compressed.gz", "g1\ng2\n"),
        write(dir, "below-limit", "b" * (limit - 1)),
        write(dir, "at-limit", "c" * limit),
        write(dir, "above-limit", "d" * (limit + 1)),
        write(dir, "big", (1 to 400).map(i => s"line-$i " + "e" * 20).mkString("\n") + "\n"),
        write(dir, "tail", "t1\nt2\n"))

      val sequential = readAll(paths, 1, limit)
      Seq(4, 16).foreach { c =>
        withClue(s"readConcurrency=$c: ") { readAll(paths, c, limit) shouldBe sequential }
      }
      // default buffer: everything fits in memory
      readAll(paths, 8) shouldBe sequential
      sequential.sorted shouldBe viaDataSource(paths).collect().map(_.getString(0)).sorted.toSeq
      waitNoReaderThreads()
    }
  }

  test("keeps the submission order even when later files finish first") {
    registerFileSystems()
    withTestTempDir { dir =>
      val n = 60
      val paths = (1 to n).map(i => "slowfs://" + write(dir, f"f$i%03d", s"$i-a\n$i-b\n"))
      // early files are slow, late files are fast
      SlowFileSystem.reset(name => (n - name.drop(1).toInt) * 2L)
      val expected = (1 to n).flatMap(i => Seq(s"$i-a", s"$i-b"))
      readAll(paths, 1) shouldBe expected
      readAll(paths, 8) shouldBe expected
      readAll(paths, 16) shouldBe expected
      waitNoReaderThreads()
    }
  }

  test("a missing file is skipped with ignoreMissingFiles and fails without it") {
    withTestTempDir { dir =>
      val a = write(dir, "a", "a1\na2\n")
      val missing = new File(dir, "missing").getAbsolutePath
      val b = write(dir, "b", "b1\n")

      Seq(1, 4).foreach { c =>
        withClue(s"readConcurrency=$c: ") {
          readAll(Seq(a, missing, b), c, ignoreMissing = true) shouldBe Seq("a1", "a2", "b1")
          intercept[Exception](readAll(Seq(a, missing, b), c))
        }
      }
      waitNoReaderThreads()
    }
  }

  test("a failing file rethrows its error in order and releases the threads") {
    registerFileSystems()
    withTestTempDir { dir =>
      val ok1 = "failfs://" + write(dir, "ok1", "1\n")
      val bad = "failfs://" + write(dir, "bad1", "x\n")
      val ok2 = "failfs://" + write(dir, "ok2", "2\n")
      val tail = (1 to 20).map(i => "failfs://" + write(dir, s"t$i", s"$i\n"))

      val reader = direct(Seq(ok1, bad, ok2) ++ tail, 4)
      reader.next().getString(0) shouldBe "1"
      val e = intercept[IOException](reader.next())
      assert(e.getMessage.contains("boom bad1"))
      reader.hasNext shouldBe false
      waitNoReaderThreads()

      val sparkError = intercept[Exception](readAll(Seq(ok1, bad, ok2), 4))
      assert(sparkError.getMessage.contains("boom bad1"))
      waitNoReaderThreads()
    }
  }

  test("closing the reader mid way stops the pool, and exhausting it does too") {
    registerFileSystems()
    withTestTempDir { dir =>
      SlowFileSystem.reset(_ => 10L)
      val paths = (1 to 40).map(i => "slowfs://" + write(dir, s"c$i", s"v$i\n"))

      val partial = direct(paths, 8)
      partial.next().getString(0) shouldBe "v1"
      readerThreads should be > 0
      partial.close()
      waitNoReaderThreads()
      partial.hasNext shouldBe false

      val full = direct(paths, 8)
      full.map(_.getString(0)).toList shouldBe (1 to 40).map(i => s"v$i")
      waitNoReaderThreads()
    }
  }

  test("never has more than readConcurrency files being opened at once") {
    registerFileSystems()
    withTestTempDir { dir =>
      SlowFileSystem.reset(_ => 15L)
      val paths = (1 to 80).map(i => "slowfs://" + write(dir, s"m$i", s"v$i\n"))
      direct(paths, 4).map(_.getString(0)).toList.size shouldBe 80
      SlowFileSystem.maxInFlight.get() should be <= 4
      SlowFileSystem.maxInFlight.get() should be > 1
      waitNoReaderThreads()
    }
  }

  test("readConcurrency 1 reads in the task thread, larger values read on reader threads") {
    registerFileSystems()
    withTestTempDir { dir =>
      val paths = (1 to 20).map(i => "slowfs://" + write(dir, s"t$i", s"v$i\n"))

      SlowFileSystem.reset(_ => 0L)
      readAll(paths, 1).size shouldBe 20
      SlowFileSystem.openThreads.asScala.exists(_.startsWith("s3conn-read")) shouldBe false

      SlowFileSystem.reset(_ => 0L)
      readAll(paths, 4).size shouldBe 20
      SlowFileSystem.openThreads.asScala.forall(_.startsWith("s3conn-read")) shouldBe true
      waitNoReaderThreads()
    }
  }

  test("16 files in flight read 20 ms opens at least 5 times faster than one at a time") {
    registerFileSystems()
    withTestTempDir { dir =>
      SlowFileSystem.reset(_ => 20L)
      val paths = (1 to 160).map(i => "slowfs://" + write(dir, s"l$i", s"v$i\n"))
      val conf = hadoopConf
      val codecs = new CompressionCodecFactory(conf)

      val t0 = System.nanoTime
      val sequentialRows = paths.iterator.flatMap(p =>
        S3ConnectorTextRelation.readLines(new Path(p), conf, codecs, ignoreMissing = false)).map(_.getString(0)).toList
      val sequentialSec = (System.nanoTime - t0) / 1e9

      val t1 = System.nanoTime
      val concurrentRows = direct(paths, 16).map(_.getString(0)).toList
      val concurrentSec = (System.nanoTime - t1) / 1e9

      concurrentRows shouldBe sequentialRows
      info(f"sequential=$sequentialSec%.2fs concurrent(16)=$concurrentSec%.2fs ratio=${sequentialSec / concurrentSec}%.1fx")
      assert(concurrentSec * 5 <= sequentialSec,
        s"expected at least 5x, got sequential=$sequentialSec concurrent=$concurrentSec")
      waitNoReaderThreads()
    }
  }
}
