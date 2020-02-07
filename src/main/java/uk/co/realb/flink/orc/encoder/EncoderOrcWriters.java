package uk.co.realb.flink.orc.encoder;

import org.apache.orc.TypeDescription;
import uk.co.realb.flink.orc.OrcWriters;

import java.util.Properties;
/**
 * @deprecated use {@link uk.co.realb.flink.orc.OrcWriters} instead.
 */
@Deprecated
public class EncoderOrcWriters {
    public static <T> EncoderOrcWriterFactory<T> withCustomEncoder(OrcRowEncoder<T> encoder,
                                                                   TypeDescription schema,
                                                                   Properties props) {
        return OrcWriters.withCustomEncoder(encoder, schema, props);
    }
}
