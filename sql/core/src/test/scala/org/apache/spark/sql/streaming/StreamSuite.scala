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

package org.apache.spark.sql.streaming

import java.io.{File, InterruptedIOException, IOException, UncheckedIOException}
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{CountDownLatch, ExecutionException, TimeUnit}

import scala.concurrent.TimeoutException
import scala.reflect.ClassTag
import scala.util.control.ControlThrowable

import com.google.common.util.concurrent.UncheckedExecutionException
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkContext, TaskContext, TestUtils}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_MILLIS
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.{LocalLimitExec, SimpleMode, SparkPlan}
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemoryStream, MemorySink}
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProvider}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.streaming.util.{BlockOnStopSourceProvider, StreamManualClock}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.Utils

class StreamSuite extends StreamTest {

  import testImplicits._

  test("map with recovery") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped)(
      AddData(inputData, 1, 2, 3),
      StartStream(),
      CheckAnswer(2, 3, 4),
      StopStream,
      AddData(inputData, 4, 5, 6),
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7))
  }

  test("join") {
    // Make a table and ensure it will be broadcast.
    val smallTable = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")

    // Join the input stream with a table.
    val inputData = MemoryStream[Int]
    val joined = inputData.toDS().toDF().join(smallTable, $"value" === $"number")

    testStream(joined)(
      AddData(inputData, 1, 2, 3),
      CheckAnswer(Row(1, 1, "one"), Row(2, 2, "two")),
      AddData(inputData, 4),
      CheckAnswer(Row(1, 1, "one"), Row(2, 2, "two"), Row(4, 4, "four")))
  }

  test("StreamingRelation.computeStats") {
    withTempDir { dir =>
      val df = spark.readStream.format("csv").schema(StructType(Seq())).load(dir.getCanonicalPath)
      val streamingRelation = df.logicalPlan collect {
        case s: StreamingRelation => s
      }
      assert(streamingRelation.nonEmpty, "cannot find StreamingRelation")
      assert(
        streamingRelation.head.computeStats.sizeInBytes ==
          spark.sessionState.conf.defaultSizeInBytes)
    }
  }

  test("StreamingRelationV2.computeStats") {
    val streamingRelation = spark.readStream.format("rate").load().logicalPlan collect {
      case s: StreamingRelationV2 => s
    }
    assert(streamingRelation.nonEmpty, "cannot find StreamingRelationV2")
    assert(
      streamingRelation.head.computeStats.sizeInBytes == spark.sessionState.conf.defaultSizeInBytes)
  }

  test("StreamingExecutionRelation.computeStats") {
    val memoryStream = MemoryStream[Int]
    val executionRelation = StreamingExecutionRelation(
      memoryStream, memoryStream.encoder.schema.toAttributes)(memoryStream.sqlContext.sparkSession)
    assert(executionRelation.computeStats.sizeInBytes == spark.sessionState.conf.defaultSizeInBytes)
  }

  test("explain join with a normal source") {
    // This test triggers CostBasedJoinReorder to call `computeStats`
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true", SQLConf.JOIN_REORDER_ENABLED.key -> "true") {
      val smallTable = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")
      val smallTable2 = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")
      val smallTable3 = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")

      // Join the input stream with a table.
      val df = spark.readStream.format("rate").load()
      val joined = df.join(smallTable, smallTable("number") === $"value")
        .join(smallTable2, smallTable2("number") === $"value")
        .join(smallTable3, smallTable3("number") === $"value")

      val outputStream = new java.io.ByteArrayOutputStream()
      Console.withOut(outputStream) {
        joined.explain(true)
      }
      assert(outputStream.toString.contains("StreamingRelation"))
    }
  }

  test("explain join with MemoryStream") {
    // This test triggers CostBasedJoinReorder to call `computeStats`
    // Because MemoryStream doesn't use DataSource code path, we need a separate test.
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true", SQLConf.JOIN_REORDER_ENABLED.key -> "true") {
      val smallTable = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")
      val smallTable2 = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")
      val smallTable3 = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")

      // Join the input stream with a table.
      val df = MemoryStream[Int].toDF
      val joined = df.join(smallTable, smallTable("number") === $"value")
        .join(smallTable2, smallTable2("number") === $"value")
        .join(smallTable3, smallTable3("number") === $"value")

      val outputStream = new java.io.ByteArrayOutputStream()
      Console.withOut(outputStream) {
        joined.explain(true)
      }
      assert(outputStream.toString.contains("StreamingRelation"))
    }
  }

  test("SPARK-20432: union one stream with itself") {
    val df = spark.readStream.format(classOf[FakeDefaultSource].getName).load().select("a")
    val unioned = df.union(df)
    withTempDir { outputDir =>
      withTempDir { checkpointDir =>
        val query =
          unioned
            .writeStream.format("parquet")
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .start(outputDir.getAbsolutePath)
        try {
          query.processAllAvailable()
          val outputDf = spark.read.parquet(outputDir.getAbsolutePath).as[Long]
          checkDatasetUnorderly[Long](outputDf, (0L to 10L).union((0L to 10L)).toArray: _*)
        } finally {
          query.stop()
        }
      }
    }
  }

  test("union two streams") {
    val inputData1 = MemoryStream[Int]
    val inputData2 = MemoryStream[Int]

    val unioned = inputData1.toDS().union(inputData2.toDS())

    testStream(unioned)(
      AddData(inputData1, 1, 3, 5),
      CheckAnswer(1, 3, 5),
      AddData(inputData2, 2, 4, 6),
      CheckAnswer(1, 2, 3, 4, 5, 6),
      StopStream,
      AddData(inputData1, 7),
      StartStream(),
      AddData(inputData2, 8),
      CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8))
  }

  test("sql queries") {
    val inputData = MemoryStream[Int]
    inputData.toDF().createOrReplaceTempView("stream")
    val evens = sql("SELECT * FROM stream WHERE value % 2 = 0")

    testStream(evens)(
      AddData(inputData, 1, 2, 3, 4),
      CheckAnswer(2, 4))
  }

  test("DataFrame reuse") {
    def assertDF(df: DataFrame): Unit = {
      withTempDir { outputDir =>
        withTempDir { checkpointDir =>
          val query = df.writeStream.format("parquet")
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .start(outputDir.getAbsolutePath)
          try {
            query.processAllAvailable()
            val outputDf = spark.read.parquet(outputDir.getAbsolutePath).sort('a).as[Long]
            checkDataset[Long](outputDf, (0L to 10L).toArray: _*)
          } finally {
            query.stop()
          }
        }
      }
    }

    val df = spark.readStream.format(classOf[FakeDefaultSource].getName).load()
    Seq("", "parquet").foreach { useV1Source =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1Source) {
        assertDF(df)
        assertDF(df)
      }
    }
  }
}

abstract class FakeSource extends StreamSourceProvider {
  private val fakeSchema = StructType(StructField("a", IntegerType) :: Nil)

  override def sourceSchema(
      spark: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = ("fakeSource", fakeSchema)
}

/** A fake StreamSourceProvider that creates a fake Source that cannot be reused. */
class FakeDefaultSource extends FakeSource {

  override def createSource(
      spark: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    // Create a fake Source that emits 0 to 10.
    new Source {
      private var offset = -1L

      override def schema: StructType = StructType(StructField("a", IntegerType) :: Nil)

      override def getOffset: Option[Offset] = {
        if (offset >= 10) {
          None
        } else {
          offset += 1
          Some(LongOffset(offset))
        }
      }

      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        val startOffset = start.map(_.asInstanceOf[LongOffset].offset).getOrElse(-1L) + 1
        val ds = new Dataset[java.lang.Long](
          spark.sparkSession,
          Range(
            startOffset,
            end.asInstanceOf[LongOffset].offset + 1,
            1,
            Some(spark.sparkSession.sparkContext.defaultParallelism),
            isStreaming = true),
          Encoders.LONG)
        ds.toDF("a")
      }

      override def stop(): Unit = {}
    }
  }
}

/** A fake source that throws the same IOException like pre Hadoop 2.8 when it's interrupted. */
class ThrowingIOExceptionLikeHadoop12074 extends FakeSource {
  import ThrowingIOExceptionLikeHadoop12074._

  override def createSource(
      spark: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    createSourceLatch.countDown()
    try {
      Thread.sleep(30000)
      throw new TimeoutException("sleep was not interrupted in 30 seconds")
    } catch {
      case ie: InterruptedException =>
        throw new IOException(ie.toString)
    }
  }
}

object ThrowingIOExceptionLikeHadoop12074 {
  /**
   * A latch to allow the user to wait until `ThrowingIOExceptionLikeHadoop12074.createSource` is
   * called.
   */
  @volatile var createSourceLatch: CountDownLatch = null
}

/** A fake source that throws InterruptedIOException like Hadoop 2.8+ when it's interrupted. */
class ThrowingInterruptedIOException extends FakeSource {
  import ThrowingInterruptedIOException._

  override def createSource(
      spark: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    createSourceLatch.countDown()
    try {
      Thread.sleep(30000)
      throw new TimeoutException("sleep was not interrupted in 30 seconds")
    } catch {
      case ie: InterruptedException =>
        val iie = new InterruptedIOException(ie.toString)
        iie.initCause(ie)
        throw iie
    }
  }
}

object ThrowingInterruptedIOException {
  /**
   * A latch to allow the user to wait until `ThrowingInterruptedIOException.createSource` is
   * called.
   */
  @volatile var createSourceLatch: CountDownLatch = null
}

class TestStateStoreProvider extends StateStoreProvider {

  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      indexOrdinal: Option[Int],
      storeConfs: StateStoreConf,
      hadoopConf: Configuration): Unit = {
    throw new Exception("Successfully instantiated")
  }

  override def stateStoreId: StateStoreId = null

  override def close(): Unit = { }

  override def getStore(version: Long): StateStore = null
}

/** A fake source that throws `ThrowingExceptionInCreateSource.exception` in `createSource` */
class ThrowingExceptionInCreateSource extends FakeSource {

  override def createSource(
    spark: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]): Source = {
    ThrowingExceptionInCreateSource.createSourceLatch.countDown()
    try {
      Thread.sleep(30000)
      throw new TimeoutException("sleep was not interrupted in 30 seconds")
    } catch {
      case _: InterruptedException =>
        throw ThrowingExceptionInCreateSource.exception
    }
  }
}

object ThrowingExceptionInCreateSource {
  /**
   * A latch to allow the user to wait until `ThrowingExceptionInCreateSource.createSource` is
   * called.
   */
  @volatile var createSourceLatch: CountDownLatch = null
  @volatile var exception: Exception = null
}
