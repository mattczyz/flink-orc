package uk.co.realb.flink.orc;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class TestBucketAssigner implements BucketAssigner<Tuple3<Integer, String, String>, String> {
    private static final long serialVersionUID = 987325769970523327L;
    @Override
    public String getBucketId(Tuple3<Integer, String, String> integerStringStringTuple3, Context context) {
        return integerStringStringTuple3.f1;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
