package uk.co.realb.flink.orc.encoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import uk.co.realb.flink.orc.OrcBuilder;
import uk.co.realb.flink.orc.OrcBulkWriterFactory;

public class EncoderOrcWriters {
    public static <T> OrcBulkWriterFactory<T> withCustomEncoder(OrcRowEncoder<T> encoder,
                                                                TypeDescription schema,
                                                                Configuration conf) {
        OrcBuilder<T> builder = stream -> {
            Writer writer = OrcFile.createWriter(
                    new Path(Integer.toString(stream.hashCode())),
                    OrcFile
                            .writerOptions(conf)
                            .setSchema(schema)
                            .fileSystem(stream));

            return EncoderOrcWriter
                    .<T>builder(writer)
                    .withEncoder(encoder)
                    .withSchema(schema)
                    .build();
        };
        return new OrcBulkWriterFactory<>(builder);
    }
}
