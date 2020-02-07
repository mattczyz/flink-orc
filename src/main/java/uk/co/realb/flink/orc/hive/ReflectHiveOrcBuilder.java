package uk.co.realb.flink.orc.hive;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import uk.co.realb.flink.orc.OrcUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class ReflectHiveOrcBuilder<T> implements HiveOrcBuilder, Serializable {
    final private Properties props;
    final private Class<T> type;

    public ReflectHiveOrcBuilder(Class<T> type, Properties props) {
        this.props = props;
        this.type = type;
    }

    @Override
    public Writer createWriter(FileSystem stream) throws IOException {
        ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(
                type, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

        return OrcFile.createWriter(
            new Path(Integer.toString(stream.hashCode())),
            OrcFile
                .writerOptions(OrcUtils.getConfiguration(this.props))
                .inspector(inspector)
                .fileSystem(stream));
    }
}
