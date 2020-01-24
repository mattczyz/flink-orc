package uk.co.realb.flink.orc;

import org.apache.flink.core.fs.FSDataOutputStream;
import java.io.IOException;

public class HadoopOutputStreamAdapter extends org.apache.hadoop.fs.FSDataOutputStream {

    final private FSDataOutputStream out;

    public HadoopOutputStreamAdapter(FSDataOutputStream out) throws IOException {
        super(out, null);
        this.out = out;
    }

    @Override
    public long getPos() throws IOException {
        return out.getPos();
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void hsync() throws IOException {
        out.sync();
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }
}
