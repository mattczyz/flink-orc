package uk.co.realb.flink.orc

import java.nio.file.Paths

import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.{
  BucketAssigner,
  StreamingFileSink
}
import org.apache.flink.streaming.api.operators.StreamSink
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.exec.vector.{
  BytesColumnVector,
  LongColumnVector,
  VectorizedRowBatch
}
import org.apache.orc.{RecordReader, TypeDescription, Writer}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import uk.co.realb.flink.orc.TestUtils.flush
import uk.co.realb.flink.orc.encoder.{EncoderOrcWriters, OrcRowEncoder}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File
import scala.util.hashing.MurmurHash3

@RunWith(classOf[JUnitRunner])
class OrcStreamingFileSinkSpec extends FlatSpec with Matchers {
  private val schemaString = """struct<x:int,y:string,z:string>"""

  private val conf = new Configuration
  conf.set("orc.compress", "SNAPPY")
  conf.set("orc.bloom.filter.columns", "x")

  private val schema = TypeDescription.fromString(schemaString)
  private val encoder = new OrcRowEncoder[(Int, String, String)]() {
    override def encodeAndAdd(
        datum: (Int, String, String),
        batch: VectorizedRowBatch
    ): Unit = {
      val row = nextIndex(batch)
      batch.cols(0).asInstanceOf[LongColumnVector].vector(row) = datum._1
      batch
        .cols(1)
        .asInstanceOf[BytesColumnVector]
        .setVal(row, datum._2.getBytes)
      batch
        .cols(2)
        .asInstanceOf[BytesColumnVector]
        .setVal(row, datum._3.getBytes)
      incrementBatchSize(batch)
    }
  }

  private val bucketAssigner =
    new BucketAssigner[(Int, String, String), String] {
      override def getBucketId(
          in: (Int, String, String),
          context: BucketAssigner.Context
      ): String = in._2

      override def getSerializer: SimpleVersionedSerializer[String] =
        SimpleVersionedStringSerializer.INSTANCE
    }

  "Orc StreamingFileSink and Orc Writer output files byte content" should "be identical" in {
    val streamingTempDir = TestUtils.tempDirectory("streaming-test-files")
    val orcTempDir = TestUtils.tempDirectory("orc-test-files")

    val testData = (0 to 10000)
      .map(i => (i, "testText", MurmurHash3.stringHash(i.toString).toString))

    val sink = StreamingFileSink
      .forBulkFormat(
        new Path(streamingTempDir.getAbsolutePath),
        EncoderOrcWriters
          .withCustomEncoder[(Int, String, String)](encoder, schema, conf)
      )
      .withBucketAssigner(bucketAssigner)
      .build()

    val testHarness =
      new OneInputStreamOperatorTestHarness[
        (Int, String, String),
        AnyRef
      ](new StreamSink[(Int, String, String)](sink), 4, 4, 3)

    testHarness.setup()
    testHarness.open()
    testData.foreach(d => testHarness.processElement(d, d._1))

    testHarness.snapshot(1L, 10001L)
    testHarness.notifyOfCompletedCheckpoint(10002L)

    val testWriterFile =
      Paths.get(orcTempDir.getAbsolutePath, "test.orc").toAbsolutePath.toString
    val writer = TestUtils.createWriter(conf, schema, testWriterFile)
    val batch: VectorizedRowBatch = schema.createRowBatch()

    testData.foreach(d => writeTestRow(d, batch, writer, encoder))
    flush(batch, writer)
    writer.close()

    val testStreamingFile = Paths
      .get(streamingTempDir.getAbsolutePath, "testText", "part-3-0")
      .toAbsolutePath
      .toString

    File(testWriterFile).toByteArray() should be(
      File(testStreamingFile).toByteArray()
    )

    TestUtils.cleanupDirectory(streamingTempDir)
    TestUtils.cleanupDirectory(orcTempDir)
  }

  "Orc StreamingFileSink" should "produce into multiple buckets" in {
    val tempDir = TestUtils.tempDirectory("streaming-test-files")
    val testData = (0 to 10000)
      .map(i =>
        (i, "testText" + i % 3, MurmurHash3.stringHash(i.toString).toString)
      )

    val sink = StreamingFileSink
      .forBulkFormat(
        new Path(tempDir.getAbsolutePath),
        EncoderOrcWriters
          .withCustomEncoder[(Int, String, String)](encoder, schema, conf)
      )
      .withBucketAssigner(bucketAssigner)
      .build()

    val testHarness =
      new OneInputStreamOperatorTestHarness[
        (Int, String, String),
        AnyRef
      ](new StreamSink[(Int, String, String)](sink), 4, 4, 0)

    testHarness.setup()
    testHarness.open()
    testData.foreach(d => testHarness.processElement(d, d._1))

    testHarness.snapshot(1L, 10001L)
    testHarness.notifyOfCompletedCheckpoint(10002L)

    val testFile = (x: Seq[String]) =>
      Paths
        .get(tempDir.getAbsolutePath, x: _*)
        .toAbsolutePath
        .toString

    val batch = schema.createRowBatch

    val result = ArrayBuffer[(Int, String, String)]()

    val reader = (x: RecordReader) =>
      while (x.nextBatch(batch)) {
        (0 until batch.size)
          .map(i => {
            val x = batch.cols(0).asInstanceOf[LongColumnVector].vector.toSeq
            val y = batch
              .cols(1)
              .asInstanceOf[BytesColumnVector]
            val z = batch
              .cols(2)
              .asInstanceOf[BytesColumnVector]
            result.append((x(i).toInt, y.toString(i), z.toString(i)))

          })
      }

    reader(
      TestUtils
        .createReader(conf, testFile(Seq("testText0", "part-0-0")))
        .rows()
    )
    reader(
      TestUtils
        .createReader(conf, testFile(Seq("testText1", "part-0-1")))
        .rows()
    )
    reader(
      TestUtils
        .createReader(conf, testFile(Seq("testText2", "part-0-2")))
        .rows()
    )
    result.sortBy(_._1) should be(testData)
    TestUtils.cleanupDirectory(tempDir)
  }

  private def writeTestRow(
      row: (Int, String, String),
      batch: VectorizedRowBatch,
      writer: Writer,
      encoder: OrcRowEncoder[(Int, String, String)]
  ): Unit = {
    encoder.encodeAndAdd(row, batch)
    if (batch.size == batch.getMaxSize) {
      flush(batch, writer)
    }
  }
}
