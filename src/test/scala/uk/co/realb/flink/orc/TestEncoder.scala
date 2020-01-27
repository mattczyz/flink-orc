package uk.co.realb.flink.orc

import org.apache.hadoop.hive.ql.exec.vector.{
  BytesColumnVector,
  LongColumnVector,
  VectorizedRowBatch
}
import uk.co.realb.flink.orc.encoder.OrcRowEncoder

class TestEncoder
    extends OrcRowEncoder[(Int, String, String)]()
    with Serializable {
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
