# Flink ORC Streaming File Sink

Adds ORC support to Flink Streaming File Sink.

## Project configuration

### Dependencies

[![](https://jitpack.io/v/mattczyz/flink-orc.svg)](https://jitpack.io/#mattczyz/flink-orc)

```
repositories {
    maven { url 'https://jitpack.io' }
}

dependencies {
    compile 'com.github.mattczyz:flink-orc:release-0.2'
    compileOnly 'org.apache.hadoop:hadoop-common:2.8.3'
}
```

## Usage

### Encoder
To configure the sink, an implementation of `OrcRowEncoder[T]` is required with logic to transform user record `T` into `ColumnVectors` and then populate `VectorizedRowBatch`.

Helper methods:
* nextIndex(batch) - returning the next row index as Int
* incrementBatchSize(batch) - completing the row and incrementing internal VectorizedRowBatch counter

e.g.

```
class Encoder extends OrcRowEncoder[(Int, String, String)]() with Serializable {
  override def encodeAndAdd(
                             datum: (Int, String, String),
                             batch: VectorizedRowBatch
                           ): Unit = {
    val row = nextIndex(batch)
    batch.cols(0).asInstanceOf[LongColumnVector].vector(row) = datum._1
    batch
      .cols(1)
      .asInstanceOf[BytesColumnVector]
      .setVal(row, datum._2.getBytes)
    batch
      .cols(2)
      .asInstanceOf[BytesColumnVector]
      .setVal(row, datum._3.getBytes)
    incrementBatchSize(batch)
  }
}
```

Visit ORC [documentation](https://orc.apache.org/docs/core-java.html) to get more information on VectorizedRowBatch.

### StreamingFileSink 
The sink is built with `writerFactory` returned from 
```EncoderOrcWriters.withCustomEncoder[(Int, String, String)](encoder, schema, props)``` 
passing encoder, output schema and additional ORC configuration.

* `[(Int, String, String)]` - input data type
* encoder - implementation of `OrcRowEncoder[T]`
* schema - ORC `TypeDescription`
* props - non-default ORC configuration as `Properties`

e.g.
```
    val props = new Properties()
    props.setProperty("orc.compress", "SNAPPY")
    props.setProperty("orc.bloom.filter.columns", "x")

    val schemaString = """struct<x:int,y:string,z:string>"""
    val schema = TypeDescription.fromString(schemaString)

    stream
      .addSink(StreamingFileSink
        .forBulkFormat(
          new Path(out),
          EncoderOrcWriters
            .withCustomEncoder[(Int, String, String)](new Encoder, schema, props)
        )
        .withBucketAssigner(new BucketAssigner)
        .build())

```
