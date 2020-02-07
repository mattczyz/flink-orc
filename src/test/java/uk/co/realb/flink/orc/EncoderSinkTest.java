package uk.co.realb.flink.orc;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.orc.TypeDescription;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.co.realb.flink.orc.encoder.EncoderOrcWriterFactory;

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

public class EncoderSinkTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private OneInputStreamOperatorTestHarness<Tuple3<Integer, String, String>, Object> testHarness;

    private TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string,z:string>");

    @Before
    public void setupTestHarness() throws Exception {
        TestTupleEncoder encoder = new TestTupleEncoder();

        Properties conf = new Properties();
        conf.setProperty("orc.compress", "SNAPPY");
        conf.setProperty("orc.bloom.filter.columns", "x");
        
        String out = folder.getRoot().getAbsolutePath();
        EncoderOrcWriterFactory<Tuple3<Integer, String, String>> writer = OrcWriters.withCustomEncoder(encoder, schema, conf);
        StreamingFileSink<Tuple3<Integer, String, String>> sink = StreamingFileSink
                .forBulkFormat(new Path(out), writer)
                .withBucketAssigner(new TestBucketAssigner())
                .build();
        testHarness = new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));
        testHarness.setup();
        testHarness.open();
    }

    @Test
    public void testJavaEncoderSink() throws Exception {
        testHarness.processElement(Tuple3.of(1, "partition", "test"), 100L);
        testHarness.processElement(Tuple3.of(2, "partition", "test2"), 101L);
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
