package uk.co.realb.flink.orc.hive;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import java.io.IOException;

public interface HiveOrcBuilder {
    Writer createWriter(FileSystem stream) throws IOException;
}
