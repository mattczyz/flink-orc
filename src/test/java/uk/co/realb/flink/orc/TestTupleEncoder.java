package uk.co.realb.flink.orc;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import uk.co.realb.flink.orc.encoder.OrcRowEncoder;

public class TestTupleEncoder extends OrcRowEncoder<Tuple3<Integer, String, String>> {
    @Override
    public void encodeAndAdd(Tuple3<Integer, String, String> datum, VectorizedRowBatch batch) {
        int row = nextIndex(batch);
        LongColumnVector col0 = (LongColumnVector) batch.cols[0];
        col0.vector[row] = datum.f0;
        BytesColumnVector col1 = (BytesColumnVector) batch.cols[1];
        col1.setVal(row, datum.f1.getBytes());
        BytesColumnVector col2 = (BytesColumnVector) batch.cols[2];
        col2.setVal(row, datum.f2.getBytes());
        incrementBatchSize(batch);
    }
}
