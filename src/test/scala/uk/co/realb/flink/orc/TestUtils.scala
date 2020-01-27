package uk.co.realb.flink.orc

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
import org.apache.orc.{OrcFile, Reader, TypeDescription, Writer}

import scala.collection.JavaConverters._

object TestUtils {
  def tempDirectory(prefix: String): File = {
    val dir = Files.createTempDirectory(prefix).toFile
    dir.deleteOnExit()
    dir
  }

  def cleanupDirectory(file: File): Unit = {
    if (Files.exists(Paths.get(file.getAbsolutePath))) {
      Files
        .walk(Paths.get(file.getAbsolutePath))
        .iterator()
        .asScala
        .toSeq
        .reverse
        .map(_.toFile())
        .foreach(_.delete())
    }
  }

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
        .writerOptions(props, new Configuration())
        .setSchema(schema)
    )
  }

  def flush(batch: VectorizedRowBatch, writer: Writer): Unit = {
    if (batch.size > 0) {
      writer.addRowBatch(batch)
      batch.reset()
    }
  }
}
