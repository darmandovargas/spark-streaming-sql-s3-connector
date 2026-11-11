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

import java.io.{File, FileOutputStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPOutputStream

import com.amazonaws.spark.sql.streaming.connector.TestUtils.withTestTempDir
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.streaming.connector.s3.S3SparkUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class CountingFileSystem extends RawLocalFileSystem {
  override def getScheme: String = "countfs"
  override def getUri: URI = URI.create("countfs:///")

  override def listStatus(f: Path): Array[FileStatus] = {
    CountingFileSystem.lists.incrementAndGet()
    super.listStatus(f)
  }
}

object CountingFileSystem {
  val lists = new AtomicInteger(0)
}

class S3ConnectorTextRelationSuite extends S3ConnectorTestBase with SharedSparkSession {

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

  private def newRelation(
      paths: Seq[String],
      filesPerTask: Int = 256,
      ignoreMissing: Boolean = false,
      minReadTasks: Int = 1) =
    new S3ConnectorTextRelation(
      spark.sqlContext,
      paths,
      textSchema,
      spark.sessionState.newHadoopConf(),
      filesPerTask,
      ignoreMissing,
      minReadTasks)

  private def viaRelation(paths: Seq[String]): DataFrame =
    S3SparkUtils.ofRows(spark, LogicalRelation(newRelation(paths), isStreaming = false))

  private def viaDataSource(paths: Seq[String]): DataFrame = {
    val relation = DataSource(
      spark,
      paths = paths,
      userSpecifiedSchema = Some(textSchema),
      className = "text",
      options = Map.empty).resolveRelation(checkFilesExist = false)
    S3SparkUtils.ofRows(spark, LogicalRelation(relation, isStreaming = false))
  }

  private def sorted(df: DataFrame): Seq[String] = df.collect().map(_.getString(0)).sorted.toSeq

  test("same rows as the text data source on edge cases") {
    withTestTempDir { dir =>
      val paths = Seq(
        write(dir, "empty", ""),
        write(dir, "one-line-no-newline", "a b c"),
        write(dir, "trailing-newline", "x\ny\n"),
        write(dir, "blank-lines", "\n\nz\n\n"),
        write(dir, "crlf", "p\r\nq\r\n"),
        write(dir, "lone-cr", "m\rn\ro"),
        write(dir, "unicode", "café 中文 🚀\nlast"),
        writeGzip(dir, "compressed.gz", "g1\ng2\n"))

      sorted(viaRelation(paths)) shouldBe sorted(viaDataSource(paths))
      viaRelation(paths).count() shouldBe viaDataSource(paths).count()
    }
  }

  test("empty path list yields no rows") {
    viaRelation(Seq.empty).count() shouldBe 0
  }

  test("keeps duplicated paths as separate reads like the data source") {
    withTestTempDir { dir =>
      val p = write(dir, "dup", "a\nb\n")
      sorted(viaRelation(Seq(p, p))) shouldBe Seq("a", "a", "b", "b")
    }
  }

  test("splits paths into tasks of filesPerTask files") {
    withTestTempDir { dir =>
      val paths = (1 to 10).map(i => write(dir, s"f$i", s"line$i\n"))
      newRelation(paths, filesPerTask = 3).buildScan().getNumPartitions shouldBe 4
      newRelation(paths, filesPerTask = 100).buildScan().getNumPartitions shouldBe 1
      sorted(viaRelation(paths)) shouldBe (1 to 10).map(i => s"line$i").sorted
    }
  }

  test("numReadTasks spreads small batches over 4 tasks per core and leaves big batches unchanged") {
    def tasks(files: Int, perTask: Int, minTasks: Int): Int =
      S3ConnectorTextRelation.numReadTasks(files, perTask, minTasks)
    def defaultTasks(files: Int, cores: Int): Int =
      tasks(files, 256, S3ConnectorTextRelation.defaultMinReadTasks(cores))

    S3ConnectorTextRelation.defaultMinReadTasks(8) shouldBe 32
    S3ConnectorTextRelation.defaultMinReadTasks(40) shouldBe 160
    S3ConnectorTextRelation.defaultMinReadTasks(0) shouldBe 4

    Seq(
      // files, cores (defaultParallelism), expected tasks
      (0, 40, 1), (1, 40, 1), (10, 40, 10), (1000, 40, 160), (3000, 40, 160), (10000, 40, 160),
      (40960, 40, 160), (100000, 40, 391), (3000000, 40, 11719),
      (0, 8, 1), (1, 8, 1), (10, 8, 10), (200, 8, 32), (1000, 8, 32), (3000, 8, 32), (8192, 8, 32),
      (10000, 8, 40), (3000000, 8, 11719)
    ).foreach { case (files, cores, expected) =>
      withClue(s"files=$files cores=$cores: ") { defaultTasks(files, cores) shouldBe expected }
    }

    // override with a larger minimum and a degenerate one
    tasks(1000, 256, 64) shouldBe 64
    tasks(1000, 256, 0) shouldBe 4
    tasks(1000, 0, 1) shouldBe 1000

    // batches of minTasks * filesPerTask files or more are exactly ceil(files / filesPerTask)
    Seq(160, 32, 100).foreach { minTasks =>
      Seq(256, 100, 1).foreach { perTask =>
        val files = minTasks * perTask
        tasks(files, perTask, minTasks) shouldBe math.ceil(files.toDouble / perTask).toInt
        tasks(files * 7 + 3, perTask, minTasks) shouldBe math.ceil((files * 7 + 3).toDouble / perTask).toInt
      }
    }
  }

  test("minReadTasks sets the number of partitions of a small batch") {
    withTestTempDir { dir =>
      val paths = (1 to 10).map(i => write(dir, s"m$i", s"line$i\n"))
      newRelation(paths, filesPerTask = 256, minReadTasks = 4).buildScan().getNumPartitions shouldBe 4
      newRelation(paths, filesPerTask = 256, minReadTasks = 100).buildScan().getNumPartitions shouldBe 10
      newRelation(paths, filesPerTask = 3, minReadTasks = 2).buildScan().getNumPartitions shouldBe 4
      sorted(S3SparkUtils.ofRows(spark,
        LogicalRelation(newRelation(paths, minReadTasks = 4), isStreaming = false))) shouldBe
        (1 to 10).map(i => s"line$i").sorted
    }
  }

  test("a missing file fails the read unless ignoreMissingFiles is set") {
    withTestTempDir { dir =>
      val present = write(dir, "present", "ok\n")
      val missing = new File(dir, "missing").getAbsolutePath

      intercept[Exception] {
        S3SparkUtils.ofRows(spark, LogicalRelation(newRelation(Seq(present, missing)), isStreaming = false)).collect()
      }

      val tolerant = S3SparkUtils.ofRows(spark,
        LogicalRelation(newRelation(Seq(present, missing), ignoreMissing = true), isStreaming = false))
      sorted(tolerant) shouldBe Seq("ok")
    }
  }

  test("does not list paths while the data source does") {
    spark.sparkContext.hadoopConfiguration.set("fs.countfs.impl", classOf[CountingFileSystem].getName)
    withTestTempDir { dir =>
      val paths = (1 to 5).map(i => "countfs://" + write(dir, s"c$i", s"v$i\n"))

      CountingFileSystem.lists.set(0)
      sorted(viaRelation(paths)) shouldBe (1 to 5).map(i => s"v$i")
      CountingFileSystem.lists.get() shouldBe 0

      sorted(viaDataSource(paths)) shouldBe (1 to 5).map(i => s"v$i")
      assert(CountingFileSystem.lists.get() > 0)
    }
  }

  test("supports only a single string column text source without special options") {
    val ok = (fmt: String, schema: StructType, parts: Seq[String], opts: Map[String, String]) =>
      S3ConnectorTextRelation.supports(fmt, schema, parts, opts)

    ok("text", textSchema, Seq.empty, Map("spark.sql.shuffle.partitions" -> "4")) shouldBe true
    ok("TEXT", textSchema, Seq.empty, Map.empty) shouldBe true
    ok("csv", textSchema, Seq.empty, Map.empty) shouldBe false
    ok("text", textSchema, Seq("dt"), Map.empty) shouldBe false
    ok("text", textSchema.add("n", IntegerType), Seq.empty, Map.empty) shouldBe false
    ok("text", StructType(Seq(StructField("n", IntegerType))), Seq.empty, Map.empty) shouldBe false
    ok("text", textSchema, Seq.empty, Map("lineSep" -> ",")) shouldBe false
    ok("text", textSchema, Seq.empty, Map("wholetext" -> "true")) shouldBe false
    ok("text", textSchema, Seq.empty, Map("recursiveFileLookup" -> "true")) shouldBe false
  }

  test("rows are plain strings") {
    withTestTempDir { dir =>
      val p = write(dir, "types", "only\n")
      viaRelation(Seq(p)).collect().toSeq shouldBe Seq(Row("only"))
      viaRelation(Seq(p)).schema shouldBe textSchema
    }
  }
}
