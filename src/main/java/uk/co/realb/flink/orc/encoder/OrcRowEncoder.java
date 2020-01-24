package uk.co.realb.flink.orc.encoder;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public abstract class OrcRowEncoder<T> {

    public abstract void encodeAndAdd(T datum, VectorizedRowBatch batch);

    public Integer nextIndex(VectorizedRowBatch batch) {
        return batch.size;
    }

    public void incrementBatchSize(VectorizedRowBatch batch) {
        batch.size += 1;
    }
}
