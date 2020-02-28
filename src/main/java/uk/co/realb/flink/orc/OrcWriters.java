package uk.co.realb.flink.orc;

import org.apache.avro.generic.GenericRecord;
import org.apache.orc.TypeDescription;
import uk.co.realb.flink.orc.encoder.CustomEncoderOrcBuilder;
import uk.co.realb.flink.orc.encoder.EncoderOrcBuilder;
import uk.co.realb.flink.orc.encoder.EncoderOrcWriterFactory;
import uk.co.realb.flink.orc.encoder.OrcRowEncoder;
import uk.co.realb.flink.orc.hive.GenericRecordHiveOrcBuilder;
import uk.co.realb.flink.orc.hive.GenericRecordOrcWriterFactory;
import uk.co.realb.flink.orc.hive.HiveOrcWriterFactory;
import uk.co.realb.flink.orc.hive.ReflectHiveOrcBuilder;

import java.io.Serializable;
import java.util.Properties;

public class OrcWriters implements Serializable {

    public static <T> HiveOrcWriterFactory<T> forReflectRecord(Class<T> type, Properties props) {
        ReflectHiveOrcBuilder<T> builder = new ReflectHiveOrcBuilder<>(type, props);
        return new HiveOrcWriterFactory<>(builder);
    }

    public static <T> EncoderOrcWriterFactory<T> withCustomEncoder(OrcRowEncoder<T> encoder,
                                                                   TypeDescription schema,
                                                                   Properties props) {
        EncoderOrcBuilder<T> builder = new CustomEncoderOrcBuilder<>(encoder, schema, props);
        return new EncoderOrcWriterFactory<>(builder);
    }

    public static <T extends GenericRecord> GenericRecordOrcWriterFactory<T> forGenericRecord(String avroSchemaString, Properties props) {
        GenericRecordHiveOrcBuilder builder = new GenericRecordHiveOrcBuilder(avroSchemaString, props);
        return new GenericRecordOrcWriterFactory<>(builder);
    }
}
