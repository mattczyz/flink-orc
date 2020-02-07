package uk.co.realb.flink.orc.hive;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.hadoop.hive.ql.io.orc.Writer;

import java.io.IOException;

public class HiveOrcWriter<T> implements BulkWriter<T> {

    final private Writer writer;

    public HiveOrcWriter(Writer orcWriter) {
        this.writer = orcWriter;
    }

    @Override
    public void addElement(T element) throws IOException {
        writer.addRow(element);
    }

    @Override
    public void flush() {}

    @Override
    public void finish() throws IOException {
        flush();
        writer.close();
    }
}
