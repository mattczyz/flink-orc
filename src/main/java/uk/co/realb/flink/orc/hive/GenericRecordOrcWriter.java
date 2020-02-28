package uk.co.realb.flink.orc.hive;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.hadoop.hive.ql.io.orc.Writer;

import java.io.IOException;

public class GenericRecordOrcWriter<T extends GenericRecord> implements BulkWriter<T> {
    final private Writer writer;

    public GenericRecordOrcWriter(Writer orcWriter) {
        this.writer = orcWriter;
    }

    @Override
    public void addElement(T element) throws IOException {
        int fieldCount = element
                    .getSchema()
                    .getFields()
                    .size();

        Object[] row = new Object[fieldCount];

        for(int i=0; i < fieldCount; i++) {
            row[i] = element.get(i);
        }

        writer.addRow(row);
    }

    @Override
    public void flush() {}

    @Override
    public void finish() throws IOException {
        writer.close();
    }
}
