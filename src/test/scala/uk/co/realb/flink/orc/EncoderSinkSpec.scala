package uk.co.realb.flink.orc

import java.nio.file.Paths
import java.util.Properties

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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
import org.apache.orc.{TypeDescription, Writer}
import org.junit.rules.TemporaryFolder
import org.junit.{Rule, Test}
import org.scalatest.matchers.should.Matchers
import uk.co.realb.flink.orc.TestUtils._
import uk.co.realb.flink.orc.encoder.OrcRowEncoder

import scala.annotation.meta.getter
import scala.util.hashing.MurmurHash3

class EncoderSinkSpec extends Matchers {

  @(Rule @getter)
  val streamingOutput = new TemporaryFolder
  @(Rule @getter)
  val orcOutput = new TemporaryFolder

  private val schemaString = """struct<x:int,y:string,z:string>"""

  private val conf = new Properties
  conf.setProperty("orc.compress", "SNAPPY")
  conf.setProperty("orc.bloom.filter.columns", "x")
  conf.setProperty("orc.stripe.size", "4194304")
  conf.setProperty("orc.create.index", "true")

  private val schema = TypeDescription.fromString(schemaString)
  private val encoder = new TestEncoder()

  private val bucketAssigner =
    new BucketAssigner[(Int, String, String), String] {
      override def getBucketId(
          in: (Int, String, String),
          context: BucketAssigner.Context
      ): String = in._2

      override def getSerializer: SimpleVersionedSerializer[String] =
        SimpleVersionedStringSerializer.INSTANCE
    }

  @Test def testCompareOrcWriterOutputWithFlink() {
    val streamingTempDir = streamingOutput.getRoot.getAbsolutePath
    val orcTempDir = orcOutput.getRoot.getAbsolutePath
    val testData = (0 to 1000000)
      .map(i => (i, "testText", MurmurHash3.stringHash(i.toString).toString))

    val sink = StreamingFileSink
      .forBulkFormat(
        new Path(streamingTempDir),
        OrcWriters
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
      Paths.get(orcTempDir, "test.orc").toAbsolutePath.toString

    val writer = createWriter(conf, schema, testWriterFile)
    val batch: VectorizedRowBatch = schema.createRowBatch()

    testData.foreach(d => writeTestRow(d, batch, writer, encoder))
    flush(batch, writer)

    writer.close()

    val testStreamingFile = Paths
      .get(streamingTempDir, "testText", "part-3-0")
      .toAbsolutePath
      .toString

    fileHash(testWriterFile) should be(
      fileHash(testStreamingFile)
    )

    val reader = TestUtils.createReader(new Configuration(), testStreamingFile)

    reader.getFileTail.getPostscript.getCompression.toString should be(
      "SNAPPY"
    )
    reader.getFileTail.getFooter.getStripesList.size() should be(4)
  }

  @Test def testSinkIntoMultipleBuckets() {
    val tempDir = streamingOutput.getRoot.getAbsolutePath
    val testData = (0 to 10000)
      .map(i =>
        (i, "testText" + i % 3, MurmurHash3.stringHash(i.toString).toString)
      )

    val sink = StreamingFileSink
      .forBulkFormat(
        new Path(tempDir),
        OrcWriters
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

    val result = testTupleReader(
      schema,
      Seq(
        testFile(Seq("testText0", "part-0-0"), tempDir),
        testFile(Seq("testText1", "part-0-1"), tempDir),
        testFile(Seq("testText2", "part-0-2"), tempDir)
      )
    )

    result.sortBy(_._1) should be(testData)
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
