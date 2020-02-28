package uk.co.realb.flink.orc.hive;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import uk.co.realb.flink.orc.StreamFileSystem;

import java.io.IOException;

public class GenericRecordOrcWriterFactory<T extends GenericRecord> implements BulkWriter.Factory<T> {
    final private HiveOrcBuilder writerBuilder;

    public GenericRecordOrcWriterFactory(HiveOrcBuilder writerBuilder) {
        this.writerBuilder = writerBuilder;
    }

    @Override
    public BulkWriter<T> create(FSDataOutputStream fsDataOutputStream) throws IOException {
        StreamFileSystem stream = new StreamFileSystem(fsDataOutputStream);
        Writer writer = writerBuilder.createWriter(stream);
        return new GenericRecordOrcWriter<>(writer);
    }
}
