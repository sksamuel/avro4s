package com.sksamuel.avro4s

import java.io.File
import java.nio.file.Path

import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput, SeekableFileInput, SeekableInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import shapeless.Lazy

import scala.util.Try

class AvroInputStream[T](in: SeekableInput)(implicit schema: Lazy[ToSchema[T]], deser: Lazy[AvroReader[T]]) {

  val datumReader = new GenericDatumReader[GenericRecord](schema.value())
  val dataFileReader = new DataFileReader[GenericRecord](in, datumReader)

  def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): T = deser.value(dataFileReader.next)
  }

  def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): Try[T] = Try(deser.value(dataFileReader.next))
  }

  def close(): Unit = in.close()
}

object AvroInputStream {
  def apply[T](bytes: Array[Byte])(implicit schema: Lazy[ToSchema[T]], deserializer: Lazy[AvroReader[T]]): AvroInputStream[T] = new AvroInputStream[T](new SeekableByteArrayInput(bytes))
  def apply[T](path: String)(implicit schema: Lazy[ToSchema[T]], deserializer: Lazy[AvroReader[T]]): AvroInputStream[T] = apply(new File(path))
  def apply[T](path: Path)(implicit schema: Lazy[ToSchema[T]], deserializer: Lazy[AvroReader[T]]): AvroInputStream[T] = apply(path.toFile)
  def apply[T](file: File)(implicit schema: Lazy[ToSchema[T]], deserializer: Lazy[AvroReader[T]]): AvroInputStream[T] = new AvroInputStream[T](new SeekableFileInput(file))
}