package uk.co.realb.flink.orc.hive;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import uk.co.realb.flink.orc.StreamFileSystem;

import java.io.IOException;

public class HiveOrcWriterFactory<T> implements BulkWriter.Factory<T> {
    final private HiveOrcBuilder writerBuilder;

    public HiveOrcWriterFactory(HiveOrcBuilder writerBuilder) {
        this.writerBuilder = writerBuilder;
    }

    @Override
    public BulkWriter<T> create(FSDataOutputStream fsDataOutputStream) throws IOException {
        StreamFileSystem stream = new StreamFileSystem(fsDataOutputStream);
        Writer writer = writerBuilder.createWriter(stream);
        return new HiveOrcWriter<>(writer);
    }
}
