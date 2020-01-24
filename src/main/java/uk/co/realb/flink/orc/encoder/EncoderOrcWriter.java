package uk.co.realb.flink.orc.encoder;

import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

public class EncoderOrcWriter<T> {
    final private Writer writer;
    final private OrcRowEncoder<T> encoder;
    final private TypeDescription schema;

    public static <T> Builder<T> builder(Writer writer) {
        return new Builder<>(writer);
    }

    private EncoderOrcWriter(Writer writer, OrcRowEncoder<T> encoder, TypeDescription schema) {
        this.writer = writer;
        this.encoder = encoder;
        this.schema = schema;
    }

    public Writer getWriter() {
        return writer;
    }

    public OrcRowEncoder<T> getEncoder() {
        return encoder;
    }

    public TypeDescription getSchema() {
        return schema;
    }

    public static class Builder<T> {
        final private Writer writer;
        private OrcRowEncoder<T> encoder;
        private TypeDescription schema;

        public Builder(Writer writer) {
            this.writer = writer;
        }

        public Builder<T> withEncoder(OrcRowEncoder<T> encoder){
            this.encoder = encoder;
            return this;
        }

        public Builder<T> withSchema(TypeDescription schema){
            this.schema = schema;
            return this;
        }

        public EncoderOrcWriter<T> build(){
            return new EncoderOrcWriter<>(writer, encoder, schema);
        }
    }
}
