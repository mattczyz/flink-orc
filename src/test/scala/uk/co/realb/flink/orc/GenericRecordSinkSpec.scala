package uk.co.realb.flink.orc

import java.nio.file.Paths
import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
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
import scala.collection.JavaConverters._
import scala.util.hashing.MurmurHash3

class GenericRecordSinkSpec extends Matchers {

  @(Rule @getter)
  val streamingOutput = new TemporaryFolder
  @(Rule @getter)
  val orcOutput = new TemporaryFolder

  private val conf = new Properties
  conf.setProperty("orc.compress", "SNAPPY")
  conf.setProperty("orc.bloom.filter.columns", "id")

  private val avroSchemaString =
    """
      |{
      |	"name": "record",
      |	"type": "record",
      |	"fields": [{
      |		"name": "x",
      |		"type": "int",
      |		"doc": ""
      |	}, {
      |		"name": "y",
      |		"type": "string",
      |		"doc": ""
      |	}, {
      |		"name": "z",
      |		"type": "string",
      |		"doc": ""
      |	}]
      |}
      |""".stripMargin

  private val avroSchema = new Schema.Parser().parse(avroSchemaString)

  private val bucketAssigner =
    new BucketAssigner[GenericRecord, String] {
      override def getBucketId(
          in: GenericRecord,
          context: BucketAssigner.Context
      ): String = in.get(1).asInstanceOf[String]

      override def getSerializer: SimpleVersionedSerializer[String] =
        SimpleVersionedStringSerializer.INSTANCE
    }

  @Test def testCompareGenericRecordOrcWriterOutputWithFlink(): Unit = {
    val streamingTempDir = streamingOutput.getRoot.getAbsolutePath
    val orcTempDir = orcOutput.getRoot.getAbsolutePath

    val testData = (0 to 10000)
      .map(i => {
        val r: GenericRecord = new GenericData.Record(avroSchema)
        r.put("x", i)
        r.put("y", "testText")
        r.put("z", MurmurHash3.stringHash(i.toString).toString)
        r
      })

    val sink = StreamingFileSink
      .forBulkFormat(
        new Path(streamingTempDir),
        OrcWriters
          .forGenericRecord[GenericRecord](avroSchemaString, conf)
      )
      .withBucketAssigner(bucketAssigner)
      .build()

    val testHarness =
      new OneInputStreamOperatorTestHarness[
        GenericRecord,
        AnyRef
      ](new StreamSink[GenericRecord](sink), 4, 4, 3)

    testHarness.setup()
    testHarness.open()
    testData.foreach(d =>
      testHarness.processElement(d, d.get(0).asInstanceOf[Int])
    )

    testHarness.snapshot(1L, 10001L)
    testHarness.notifyOfCompletedCheckpoint(10002L)

    val testWriterFile =
      Paths.get(orcTempDir, "test.orc").toAbsolutePath.toString
    val writer = TestUtils
      .createGenericRecordWriter(avroSchema, conf, testWriterFile)

    testData.foreach(d =>
      writer.addRow(
        d.getSchema.getFields.asScala.map(f => d.get(f.name())).asJava
      )
    )
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
