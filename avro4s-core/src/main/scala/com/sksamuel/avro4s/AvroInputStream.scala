package com.sksamuel.avro4s

import java.io.File
import java.nio.file.Path
import java.io.EOFException

import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput, SeekableFileInput, SeekableInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

import scala.util.Try

trait AvroInputStream[T] {
  def close(): Unit
  def iterator(): Iterator[T]
  def tryIterator(): Iterator[Try[T]]
}

class AvroBinaryInputStream[T](in: SeekableByteArrayInput)(implicit schemaFor: SchemaFor[T], fromRecord: FromRecord[T])
  extends AvroInputStream[T] {

  val datumReader = new GenericDatumReader[GenericRecord](schemaFor())
  val binDecoder = DecoderFactory.get().binaryDecoder(in, null)
  val records = new java.util.ArrayList[GenericRecord]()

  try {
    while (true)
      records.add(datumReader.read(null, binDecoder))
  } catch {
    case _: EOFException => null
  }
  val result = records.iterator()

  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = result.hasNext
    override def next(): T = fromRecord(result.next)
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = result.hasNext
    override def next(): Try[T] = Try(fromRecord(result.next))
  }

  override def close(): Unit = in.close()
}

class AvroDataInputStream[T](in: SeekableInput)(implicit schemaFor: SchemaFor[T], fromRecord: FromRecord[T])
  extends AvroInputStream[T] {

  val datumReader = new GenericDatumReader[GenericRecord](schemaFor())
  val dataFileReader = new DataFileReader[GenericRecord](in, datumReader)

  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): T = fromRecord(dataFileReader.next)
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): Try[T] = Try(fromRecord(dataFileReader.next))
  }

  override def close(): Unit = in.close()
}

object AvroInputStream {
  def apply[T: SchemaFor : FromRecord](bytes: Array[Byte]): AvroInputStream[T] = new AvroBinaryInputStream[T](new SeekableByteArrayInput(bytes))
  def apply[T: SchemaFor : FromRecord](path: String): AvroInputStream[T] = apply(new File(path))
  def apply[T: SchemaFor : FromRecord](path: Path): AvroInputStream[T] = apply(path.toFile)
  def apply[T: SchemaFor : FromRecord](file: File): AvroInputStream[T] = new AvroDataInputStream[T](new SeekableFileInput (file))
}