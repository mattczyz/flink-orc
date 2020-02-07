package uk.co.realb.flink.orc.encoder;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import uk.co.realb.flink.orc.StreamFileSystem;

import java.io.IOException;

public class EncoderOrcWriterFactory<T> implements BulkWriter.Factory<T> {
    final private EncoderOrcBuilder<T> writerBuilder;

    public EncoderOrcWriterFactory(EncoderOrcBuilder<T> writerBuilder) {
        this.writerBuilder = writerBuilder;
    }

    @Override
    public BulkWriter<T> create(FSDataOutputStream fsDataOutputStream) throws IOException {
        StreamFileSystem stream = new StreamFileSystem(fsDataOutputStream);
        EncoderWriter<T> writer = writerBuilder.createWriter(stream);
        return new EncoderOrcWriter<>(writer);
    }
}
