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
import org.junit.rules.TemporaryFolder
import org.junit.{Rule, Test}
import org.scalatest.matchers.should.Matchers
import uk.co.realb.flink.orc.TestUtils.fileHash

import scala.annotation.meta.getter
import scala.util.hashing.MurmurHash3

class ReflectSinkSpec extends Matchers {

  @(Rule @getter)
  val streamingOutput = new TemporaryFolder
  @(Rule @getter)
  val orcOutput = new TemporaryFolder

  private val conf = new Properties
  conf.setProperty("orc.compress", "SNAPPY")
  conf.setProperty("orc.bloom.filter.columns", "id")

  private val bucketAssigner =
    new BucketAssigner[TestData, String] {
      override def getBucketId(
          in: TestData,
          context: BucketAssigner.Context
      ): String = in.y

      override def getSerializer: SimpleVersionedSerializer[String] =
        SimpleVersionedStringSerializer.INSTANCE
    }

  @Test def testComparePOJOOrcWriterOutputWithFlink(): Unit = {
    val streamingTempDir = streamingOutput.getRoot.getAbsolutePath
    val orcTempDir = orcOutput.getRoot.getAbsolutePath

    val testData = (0 to 10000)
      .map(i =>
        TestData(
          i,
          "testText",
          MurmurHash3.stringHash(i.toString).toString
        )
      )

    val sink = StreamingFileSink
      .forBulkFormat(
        new Path(streamingTempDir),
        OrcWriters
          .forReflectRecord[TestData](classOf[TestData], conf)
      )
      .withBucketAssigner(bucketAssigner)
      .build()

    val testHarness =
      new OneInputStreamOperatorTestHarness[
        TestData,
        AnyRef
      ](new StreamSink[TestData](sink), 4, 4, 3)

    testHarness.setup()
    testHarness.open()
    testData.foreach(d => testHarness.processElement(d, d.x))

    testHarness.snapshot(1L, 10001L)
    testHarness.notifyOfCompletedCheckpoint(10002L)

    val testWriterFile =
      Paths.get(orcTempDir, "test.orc").toAbsolutePath.toString
    val writer = TestUtils
      .createHiveWriter[TestData](conf, classOf[TestData], testWriterFile)

    testData.foreach(d => writer.addRow(d))
    writer.close()

    val testStreamingFile = Paths
      .get(streamingTempDir, "testText", "part-3-0")
      .toAbsolutePath
      .toString

    fileHash(testWriterFile) should be(
      fileHash(testStreamingFile)
    )
  }
}
