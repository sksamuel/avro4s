package com.sksamuel.avro4s

import java.io.File
import java.nio.file.{Files, Path}
import java.io.EOFException

import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

import scala.util.Try

class AvroInputStream[T](in: SeekableByteArrayInput)(implicit schemaFor: SchemaFor[T], fromRecord: FromRecord[T]) {
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

  def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = result.hasNext

    override def next(): T = fromRecord(result.next)
  }

  def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = result.hasNext

    override def next(): Try[T] = Try(fromRecord(result.next))
  }

  def close(): Unit = in.close()
}

object AvroInputStream {
  def apply[T: SchemaFor : FromRecord](bytes: Array[Byte]): AvroInputStream[T] = new AvroInputStream[T](new SeekableByteArrayInput(bytes))

  def apply[T: SchemaFor : FromRecord](path: String): AvroInputStream[T] = apply(new File(path))

  def apply[T: SchemaFor : FromRecord](path: Path): AvroInputStream[T] = apply(path.toFile)

  def apply[T: SchemaFor : FromRecord](file: File): AvroInputStream[T] = new AvroInputStream[T](new SeekableByteArrayInput(Files.readAllBytes(file.toPath)))
}