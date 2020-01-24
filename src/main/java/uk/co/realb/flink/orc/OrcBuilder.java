package uk.co.realb.flink.orc;

import org.apache.hadoop.fs.FileSystem;
import uk.co.realb.flink.orc.encoder.EncoderOrcWriter;
import java.io.IOException;

public interface OrcBuilder<T> {
    EncoderOrcWriter<T> createWriter(FileSystem stream) throws IOException;
}
