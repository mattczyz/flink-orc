package uk.co.realb.flink.orc.encoder;

import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;

public interface EncoderOrcBuilder<T> {
    EncoderWriter<T> createWriter(FileSystem stream) throws IOException;
}
