package com.sksamuel.avro4s

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericRecord, GenericDatumWriter}

class AvroOutputStream[T](os: OutputStream)(implicit s: AvroSchema[T], w: AvroSerializer[T]) {

  val datumWriter = new GenericDatumWriter[GenericRecord](s.schema)
  val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
  dataFileWriter.create(s.schema, os)

  def write(ts: Seq[T]): Unit = {
    ts.foreach(t => {
      val record = w.write(t)
      dataFileWriter.append(record)
    })
  }

  def close(): Unit = {
    dataFileWriter.flush()
    dataFileWriter.close()
  }
}

object AvroOutputStream {
  def apply[T](file: File)(implicit s: AvroSchema[T], w: AvroSerializer[T]): AvroOutputStream[T] = apply(file.toPath)
  def apply[T](path: Path)(implicit s: AvroSchema[T], w: AvroSerializer[T]): AvroOutputStream[T] = {
    new AvroOutputStream[T](Files.newOutputStream(path))
  }
}