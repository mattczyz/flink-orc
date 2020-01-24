package uk.co.realb.flink.orc;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.co.realb.flink.orc.encoder.EncoderOrcWriters;

import java.io.File;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OrcStreamingFileSinkTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private OneInputStreamOperatorTestHarness<Tuple3<Integer, String, String>, Object> testHarness;

    @Before
    public void setupTestHarness() throws Exception {
        TestTupleEncoder encoder = new TestTupleEncoder();
        TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string,z:string>");
        Configuration conf = new Configuration();
        conf.set("orc.compress", "SNAPPY");
        conf.set("orc.bloom.filter.columns", "x");
        File out = folder.getRoot();
        OrcBulkWriterFactory<Tuple3<Integer, String, String>> writer = EncoderOrcWriters.withCustomEncoder(encoder, schema, conf);
        StreamingFileSink<Tuple3<Integer, String, String>> sink = StreamingFileSink
                .forBulkFormat(new Path(out.getAbsolutePath()), writer)
                .withBucketAssigner(new TestBucketAssigner())
                .build();
        testHarness = new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));
        testHarness.setup();
        testHarness.open();
    }

    @Test
    public void buildOrcStreamingFileSink() throws Exception {
        testHarness.processElement(Tuple3.of(1, "partition", "test"), 100L);
        testHarness.snapshot(1L, 10001L);
        testHarness.notifyOfCompletedCheckpoint(10002L);
        File result = Paths.get(folder.getRoot().getAbsolutePath(), "partition", "part-0-0").toFile();
        assertTrue(result.exists());
        assertEquals(889, result.length());
    }
}
