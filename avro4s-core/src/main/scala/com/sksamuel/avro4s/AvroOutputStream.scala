package com.sksamuel.avro4s

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}

class AvroOutputStream[T](os: OutputStream)(implicit schemaFor: SchemaFor[T], writer: ToRecord[T]) {

  val datumWriter = new GenericDatumWriter[GenericRecord]()
  val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
  dataFileWriter.create(schemaFor(), os)

  def write(ts: Seq[T]): Unit = ts.foreach(write)

  def write(t: T): Unit = {
    val record = writer(t)
    dataFileWriter.append(record)
  }

  def flush(): Unit = dataFileWriter.flush()

  def fSync(): Unit = dataFileWriter.fSync()

  def close(): Unit = {
    dataFileWriter.flush()
    dataFileWriter.close()
  }
}

object AvroOutputStream {
  def apply[T: SchemaFor : ToRecord](file: File): AvroOutputStream[T] = apply(file.toPath)
  def apply[T: SchemaFor : ToRecord](path: Path): AvroOutputStream[T] = apply(Files.newOutputStream(path))
  def apply[T: SchemaFor : ToRecord](os: OutputStream): AvroOutputStream[T] = new AvroOutputStream[T](os)
}