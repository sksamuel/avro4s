package com.sksamuel.avro4s

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericRecord, GenericDatumWriter}

class AvroOutputStream[T](os: OutputStream)(implicit s: AvroSchema[T], w: AvroSerializer[T]) {

  val datumWriter = new GenericDatumWriter[GenericRecord](s.schema)
  val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
  dataFileWriter.create(s.schema, os)

  def write(ts: Seq[T]): Unit = ts.foreach(write)

  def write(t: T): Unit = {
    val record = w.write(t)
    dataFileWriter.append(record)
  }

  def flush(): Unit = dataFileWriter.flush()

  def close(): Unit = {
    dataFileWriter.flush()
    dataFileWriter.close()
  }
}

object AvroOutputStream {
  def apply[T: AvroSchema : AvroSerializer](file: File): AvroOutputStream[T] = apply(file.toPath)
  def apply[T: AvroSchema : AvroSerializer](path: Path): AvroOutputStream[T] = apply(Files.newOutputStream(path))
  def apply[T: AvroSchema : AvroSerializer](os: OutputStream): AvroOutputStream[T] = new AvroOutputStream[T](os)
}