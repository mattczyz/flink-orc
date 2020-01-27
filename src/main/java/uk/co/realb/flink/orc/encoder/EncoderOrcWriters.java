package uk.co.realb.flink.orc.encoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import uk.co.realb.flink.orc.OrcBuilder;
import uk.co.realb.flink.orc.OrcBulkWriterFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class EncoderOrcWriters implements Serializable {
    public static <T> OrcBulkWriterFactory<T> withCustomEncoder(OrcRowEncoder<T> encoder,
                                                                TypeDescription schema,
                                                                Properties props) {
        OrcBuilder<T> builder = new EncoderOrcBuilder<>(encoder, schema, props);
        return new OrcBulkWriterFactory<>(builder);
    }

    public static class EncoderOrcBuilder<T> implements OrcBuilder<T>, Serializable {
        final private Properties props;
        final private TypeDescription schema;
        final private OrcRowEncoder<T> encoder;

        public EncoderOrcBuilder(OrcRowEncoder<T> encoder, TypeDescription schema, Properties props) {
            this.props = props;
            this.schema = schema;
            this.encoder = encoder;
        }

        @Override
        public EncoderOrcWriter<T> createWriter(FileSystem stream) throws IOException {
            Configuration config = new Configuration();
            props.stringPropertyNames()
                    .stream()
                    .filter(p -> props.getProperty(p) != null)
                    .forEach(p -> config.set(p, props.getProperty(p)));

            Writer writer = OrcFile.createWriter(
                    new Path(Integer.toString(stream.hashCode())),
                    OrcFile
                            .writerOptions(config)
                            .setSchema(schema)
                            .fileSystem(stream));

            return EncoderOrcWriter
                    .<T>builder(writer)
                    .withEncoder(encoder)
                    .withSchema(schema)
                    .build();
        }
    }
}
