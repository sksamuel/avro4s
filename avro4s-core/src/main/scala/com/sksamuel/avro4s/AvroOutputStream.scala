package com.sksamuel.avro4s

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import shapeless.Lazy

class AvroOutputStream[T](os: OutputStream)(implicit schema: Lazy[AvroSchema[T]], ser: Lazy[AvroSerializer[T]]) {

  val datumWriter = new GenericDatumWriter[GenericRecord](schema.value.apply)
  val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
  dataFileWriter.create(schema.value.apply, os)

  def write(ts: Seq[T]): Unit = ts.foreach(write)

  def write(t: T): Unit = {
    val record = ser.value.toRecord(t)
    println(record)
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
  def apply[T](file: File)(implicit schema: Lazy[AvroSchema[T]], ser: Lazy[AvroSerializer[T]]): AvroOutputStream[T] = apply(file.toPath)
  def apply[T](path: Path)(implicit schema: Lazy[AvroSchema[T]], ser: Lazy[AvroSerializer[T]]): AvroOutputStream[T] = apply(Files.newOutputStream(path))
  def apply[T](os: OutputStream)(implicit schema: Lazy[AvroSchema[T]], ser: Lazy[AvroSerializer[T]]): AvroOutputStream[T] = new AvroOutputStream[T](os)
}