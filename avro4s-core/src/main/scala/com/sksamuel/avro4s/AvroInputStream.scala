package com.sksamuel.avro4s

import java.io.File
import java.nio.file.Path

import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput, SeekableFileInput, SeekableInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import scala.util.Try

class AvroInputStream[T](in: SeekableInput)(implicit schemaFor: SchemaFor[T], reader: AvroReader[T]) {

  val datumReader = new GenericDatumReader[GenericRecord](schemaFor())
  val dataFileReader = new DataFileReader[GenericRecord](in, datumReader)

  def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): T = reader(dataFileReader.next)
  }

  def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): Try[T] = Try(reader(dataFileReader.next))
  }

  def close(): Unit = in.close()
}

object AvroInputStream {
  def apply[T: SchemaFor : AvroReader](bytes: Array[Byte]): AvroInputStream[T] = new AvroInputStream[T](new SeekableByteArrayInput(bytes))
  def apply[T: SchemaFor : AvroReader](path: String): AvroInputStream[T] = apply(new File(path))
  def apply[T: SchemaFor : AvroReader](path: Path): AvroInputStream[T] = apply(path.toFile)
  def apply[T: SchemaFor : AvroReader](file: File): AvroInputStream[T] = new AvroInputStream[T](new SeekableFileInput(file))
}