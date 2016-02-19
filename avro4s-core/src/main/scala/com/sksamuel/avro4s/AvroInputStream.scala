package com.sksamuel.avro4s

import java.io.File
import java.nio.file.Path

import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput, SeekableFileInput, SeekableInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import scala.util.Try

class AvroInputStream[T](in: SeekableInput)(implicit schemaFor: SchemaFor[T], fromRecord: FromRecord[T]) {

  val datumReader = new GenericDatumReader[GenericRecord](schemaFor())
  val dataFileReader = new DataFileReader[GenericRecord](in, datumReader)

  def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): T = fromRecord(dataFileReader.next)
  }

  def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): Try[T] = Try(fromRecord(dataFileReader.next))
  }

  def close(): Unit = in.close()
}

object AvroInputStream {
  def apply[T: SchemaFor : FromRecord](bytes: Array[Byte]): AvroInputStream[T] = new AvroInputStream[T](new SeekableByteArrayInput(bytes))
  def apply[T: SchemaFor : FromRecord](path: String): AvroInputStream[T] = apply(new File(path))
  def apply[T: SchemaFor : FromRecord](path: Path): AvroInputStream[T] = apply(path.toFile)
  def apply[T: SchemaFor : FromRecord](file: File): AvroInputStream[T] = new AvroInputStream[T](new SeekableFileInput(file))
}