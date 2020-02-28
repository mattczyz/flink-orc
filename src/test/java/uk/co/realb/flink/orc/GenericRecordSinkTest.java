package uk.co.realb.flink.orc;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.orc.TypeDescription;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.co.realb.flink.orc.hive.GenericRecordOrcWriterFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static scala.collection.JavaConversions.asScalaBuffer;
import static scala.collection.JavaConversions.seqAsJavaList;

public class GenericRecordSinkTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private OneInputStreamOperatorTestHarness<GenericRecord, Object> testHarness;

    private TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string,z:string>");

    private String avroSchemaString = "" +
            "{\n" +
            "\t\"name\": \"record\",\n" +
            "\t\"type\": \"record\",\n" +
            "\t\"fields\": [{\n" +
            "\t\t\"name\": \"x\",\n" +
            "\t\t\"type\": \"int\",\n" +
            "\t\t\"doc\": \"x\"\n" +
            "\t}, {\n" +
            "\t\t\"name\": \"y\",\n" +
            "\t\t\"type\": \"string\",\n" +
            "\t\t\"doc\": \"y\"\n" +
            "\t}, {\n" +
            "\t\t\"name\": \"z\",\n" +
            "\t\t\"type\": \"string\",\n" +
            "\t\t\"doc\": \"z\"\n" +
            "\t}]\n" +
            "}";

    @Before
    public void setupTestHarness() throws Exception {
        Properties conf = new Properties();
        conf.setProperty("orc.compress", "SNAPPY");
        conf.setProperty("orc.bloom.filter.columns", "x");

        String out = folder.getRoot().getAbsolutePath();
        GenericRecordOrcWriterFactory<GenericRecord> writer = OrcWriters.forGenericRecord(avroSchemaString, conf);
        StreamingFileSink<GenericRecord> sink = StreamingFileSink
            .forBulkFormat(new Path(out), writer)
            .withBucketAssigner(new BucketAssigner<GenericRecord, String>() {
                @Override
                public String getBucketId(GenericRecord javaTestData, Context context) {
                    return javaTestData.get(1).toString();
                }

                @Override
                public SimpleVersionedSerializer<String> getSerializer() {
                    return SimpleVersionedStringSerializer.INSTANCE;
                }
            })
            .build();
        testHarness = new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));
        testHarness.setup();
        testHarness.open();
    }

    @Test
    public void testJavaGenericRecordSink() throws Exception {

        Schema avroSchema = new Schema.Parser().parse(avroSchemaString);

        GenericRecord r = new GenericData.Record(avroSchema);
        r.put("x", 1);
        r.put("y", "partition");
        r.put("z", "test");
        testHarness.processElement(r, 100L);

        GenericRecord r2 = new GenericData.Record(avroSchema);
        r2.put("x", 2);
        r2.put("y", "partition");
        r2.put("z", "test2");
        testHarness.processElement(r2, 101L);

        testHarness.snapshot(1L, 10001L);
        testHarness.notifyOfCompletedCheckpoint(10002L);
        
        File result = Paths.get(folder.getRoot().getAbsolutePath(), "partition", "part-0-0").toFile();

        String tempDir = folder.getRoot().getAbsolutePath();

        String paths = TestUtils.testFile(asScalaBuffer(Arrays.asList("partition", "part-0-0")), tempDir);

        List<scala.Tuple3<Object, String, String>> rows = seqAsJavaList(TestUtils.testTupleReader(
                schema,
                asScalaBuffer(Collections.singletonList(paths))
        ));

        assertTrue(result.exists());
        assertEquals(2, rows.size());
        assertEquals(2, rows.get(1)._1());
        assertEquals("partition", rows.get(1)._2());
        assertEquals("test2", rows.get(1)._3());
    }
}
