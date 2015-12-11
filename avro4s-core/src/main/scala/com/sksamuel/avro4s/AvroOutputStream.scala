package com.sksamuel.avro4s

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericRecord, GenericDatumWriter}

class AvroOutputStream[T](os: OutputStream)(implicit schema: AvroSchema2[T], ser: AvroSerializer[T]) {

  val datumWriter = new GenericDatumWriter[GenericRecord](schema())
  val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
  dataFileWriter.create(schema(), os)

  def write(ts: Seq[T]): Unit = ts.foreach(write)

  def write(t: T): Unit = {
    val record = ser.toRecord(t)
    println(record)
    dataFileWriter.append(record)
  }

  def flush(): Unit = dataFileWriter.flush()

  def close(): Unit = {
    dataFileWriter.flush()
    dataFileWriter.close()
  }
}

object AvroOutputStream {
  def apply[T: AvroSchema2 : AvroSerializer](file: File): AvroOutputStream[T] = apply(file.toPath)
  def apply[T: AvroSchema2 : AvroSerializer](path: Path): AvroOutputStream[T] = apply(Files.newOutputStream(path))
  def apply[T: AvroSchema2 : AvroSerializer](os: OutputStream): AvroOutputStream[T] = new AvroOutputStream[T](os)
}