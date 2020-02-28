package uk.co.realb.flink.orc.hive;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroObjectInspectorGenerator;
import uk.co.realb.flink.orc.OrcUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class GenericRecordHiveOrcBuilder implements HiveOrcBuilder, Serializable {
    final private Properties props;
    final private String schemaString;

    public GenericRecordHiveOrcBuilder(String schemaString, Properties props) {
        this.props = props;
        this.schemaString = schemaString;
    }

    @Override
    public Writer createWriter(FileSystem stream) throws IOException {
        try {
            Schema schema = new Schema.Parser().parse(schemaString);
            AvroObjectInspectorGenerator generator = new AvroObjectInspectorGenerator(schema);

            return OrcFile.createWriter(
                    new Path(Integer.toString(stream.hashCode())),
                    OrcFile
                            .writerOptions(OrcUtils.getConfiguration(this.props))
                            .inspector(generator.getObjectInspector())
                            .fileSystem(stream));
        } catch (SerDeException e) {
            throw new IOException(e);
        }
    }
}
