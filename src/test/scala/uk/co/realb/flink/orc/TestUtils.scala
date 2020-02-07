package uk.co.realb.flink.orc

import java.nio.file.Paths
import java.security.MessageDigest
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.hive.ql.exec.vector.{
  BytesColumnVector,
  LongColumnVector,
  VectorizedRowBatch
}
import org.apache.hadoop.hive.ql.io.orc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.orc.{OrcFile, Reader, TypeDescription, Writer}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File

object TestUtils {
  def createReader(conf: Configuration, testFile: String): Reader = {
    OrcFile.createReader(
      new fs.Path(testFile),
      OrcFile.readerOptions(conf)
    )
  }

  def createWriter(
      props: Properties,
      schema: TypeDescription,
      testFile: String
  ): Writer = {
    OrcFile.createWriter(
      new fs.Path(testFile),
      OrcFile
        .writerOptions(OrcUtils.getConfiguration(props))
        .setSchema(schema)
    )
  }

  def createHiveWriter[T](
      props: Properties,
      classType: Class[T],
      testFile: String
  ): orc.Writer = {

    val inspector = ObjectInspectorFactory.getReflectionObjectInspector(
      classType,
      ObjectInspectorFactory.ObjectInspectorOptions.JAVA
    )

    val writer: orc.Writer =
      org.apache.hadoop.hive.ql.io.orc.OrcFile.createWriter(
        new fs.Path(testFile),
        org.apache.hadoop.hive.ql.io.orc.OrcFile
          .writerOptions(props, new Configuration())
          .inspector(inspector)
      )
    writer
  }

  def testTupleReader(
      schema: TypeDescription,
      testFiles: Seq[String]
  ): Seq[(Int, String, String)] = {
    val result = ArrayBuffer[(Int, String, String)]()
    testFiles.foreach(testFile => {
      val batch = schema.createRowBatch
      val rr = createReader(
        new Configuration(),
        testFile
      ).rows()

      while (rr.nextBatch(batch)) {
        (0 until batch.size)
          .map(i => {
            val x = batch.cols(0).asInstanceOf[LongColumnVector].vector.toSeq
            val y = batch
              .cols(1)
              .asInstanceOf[BytesColumnVector]
            val z = batch
              .cols(2)
              .asInstanceOf[BytesColumnVector]
            result.append((x(i).toInt, y.toString(i), z.toString(i)))

          })
      }
    })

    result
  }

  def testFile(x: Seq[String], tempDir: String): String = {
    Paths
      .get(tempDir, x: _*)
      .toAbsolutePath
      .toString
  }

  def flush(batch: VectorizedRowBatch, writer: Writer): Unit = {
    if (batch.size > 0) {
      writer.addRowBatch(batch)
      batch.reset()
    }
  }

  def fileHash(file: String): String =
    MessageDigest
      .getInstance("MD5")
      .digest(File(file).toByteArray())
      .map("%02x".format(_))
      .mkString
}
