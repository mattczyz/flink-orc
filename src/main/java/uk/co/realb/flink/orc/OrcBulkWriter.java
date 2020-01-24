package uk.co.realb.flink.orc;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import uk.co.realb.flink.orc.encoder.EncoderOrcWriter;
import uk.co.realb.flink.orc.encoder.OrcRowEncoder;
import java.io.IOException;

public class OrcBulkWriter<T> implements BulkWriter<T> {

    final private OrcRowEncoder<T> encoder;
    final private Writer writer;
    final private VectorizedRowBatch buffer;

    public OrcBulkWriter(EncoderOrcWriter<T> orcWriter) {
        TypeDescription typeDescription = orcWriter.getSchema();
        this.encoder = orcWriter.getEncoder();
        this.writer = orcWriter.getWriter();
        this.buffer = typeDescription.createRowBatch();
    }

    @Override
    public void addElement(T element) throws IOException {
        encoder.encodeAndAdd(element, buffer);
        if (buffer.size == buffer.getMaxSize()) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException {
        if (buffer.size > 0) {
            writer.addRowBatch(buffer);
            buffer.reset();
        }
    }

    @Override
    public void finish() throws IOException {
        flush();
        writer.close();
    }
}
