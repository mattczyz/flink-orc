package uk.co.realb.flink.orc.encoder;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import uk.co.realb.flink.orc.OrcUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class CustomEncoderOrcBuilder<T> implements EncoderOrcBuilder<T>, Serializable {
    final private Properties props;
    final private TypeDescription schema;
    final private OrcRowEncoder<T> encoder;

    public CustomEncoderOrcBuilder(OrcRowEncoder<T> encoder, TypeDescription schema, Properties props) {
        this.props = props;
        this.schema = schema;
        this.encoder = encoder;
    }

    @Override
    public EncoderWriter<T> createWriter(FileSystem stream) throws IOException {
        Writer writer = org.apache.orc.OrcFile.createWriter(
            new Path(Integer.toString(stream.hashCode())),
                OrcFile
                    .writerOptions(OrcUtils.getConfiguration(this.props))
                    .setSchema(schema)
                    .fileSystem(stream));

        return EncoderWriter
                .<T>builder(writer)
                .withEncoder(encoder)
                .withSchema(schema)
                .build();
    }
}
