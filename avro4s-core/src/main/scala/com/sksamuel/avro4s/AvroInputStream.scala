package com.sksamuel.avro4s

import java.io.File
import java.nio.file.{Files, Path}

import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

import scala.util.Try

class AvroInputStream[T](in: SeekableByteArrayInput)(implicit schemaFor: SchemaFor[T], fromRecord: FromRecord[T]) {

  val datumReader = new GenericDatumReader[GenericRecord](schemaFor())
  val binDecoder = DecoderFactory.get().binaryDecoder(in, null)

  def iterator: Iterator[T] =
    Iterator.continually(Try(datumReader.read(null, binDecoder)))
      .takeWhile(_.isSuccess)
      .map(_.get)
      .map(fromRecord.apply)

  def tryIterator: Iterator[Try[T]] =
    Iterator.continually(Try(datumReader.read(null, binDecoder)))
      .takeWhile(_.isSuccess)
      .map(_.get)
      .map(record => Try(fromRecord(record)))

  def close(): Unit = in.close()
}

object AvroInputStream {
  def apply[T: SchemaFor : FromRecord](bytes: Array[Byte]): AvroInputStream[T] = new AvroInputStream[T](new SeekableByteArrayInput(bytes))
  def apply[T: SchemaFor : FromRecord](path: String): AvroInputStream[T] = apply(new File(path))
  def apply[T: SchemaFor : FromRecord](path: Path): AvroInputStream[T] = apply(path.toFile)
  def apply[T: SchemaFor : FromRecord](file: File): AvroInputStream[T] = new AvroInputStream[T](new SeekableByteArrayInput(Files.readAllBytes(file.toPath)))
}