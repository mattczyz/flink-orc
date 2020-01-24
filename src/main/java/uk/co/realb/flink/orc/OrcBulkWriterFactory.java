package uk.co.realb.flink.orc;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import uk.co.realb.flink.orc.encoder.EncoderOrcWriter;

import java.io.IOException;

public class OrcBulkWriterFactory<T> implements BulkWriter.Factory<T> {
    final private OrcBuilder<T> writerBuilder;

    public OrcBulkWriterFactory(OrcBuilder<T> writerBuilder) {
        this.writerBuilder = writerBuilder;
    }

    @Override
    public BulkWriter<T> create(FSDataOutputStream fsDataOutputStream) throws IOException {
        StreamFileSystem stream = new StreamFileSystem(fsDataOutputStream);
        EncoderOrcWriter<T> writer = writerBuilder.createWriter(stream);
        return new OrcBulkWriter<>(writer);
    }
}
