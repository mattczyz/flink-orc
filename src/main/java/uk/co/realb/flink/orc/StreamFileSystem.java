package uk.co.realb.flink.orc;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

public class StreamFileSystem extends FileSystem {
    final private org.apache.flink.core.fs.FSDataOutputStream out;
    public StreamFileSystem(org.apache.flink.core.fs.FSDataOutputStream out) {
        this.out = out;
    }

    @Override
    public URI getUri() {
        return null;
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        return null;
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable) throws IOException {
        return new HadoopOutputStreamAdapter(out);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        return null;
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        return false;
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        return false;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path path) {

    }

    @Override
    public Path getWorkingDirectory() {
        return null;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        return false;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return null;
    }
}
