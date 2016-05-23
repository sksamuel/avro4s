package com.sksamuel.avro4s

import java.io.{File, InputStream}
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

final case class AvroJsonInputStream[T](in: InputStream)(implicit schemaFor: SchemaFor[T], fromRecord: FromRecord[T]) {
  private val schema = schemaFor()
  private val dataumReader = new GenericDatumReader[GenericRecord](schema)
  private val jsonDecoder = DecoderFactory.get.jsonDecoder(schema, in)

  def iterator: Iterator[T] = Iterator.continually(Try{dataumReader.read(null, jsonDecoder)})
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(fromRecord.apply)

  def tryIterator: Iterator[Try[T]] = Iterator.continually(Try(dataumReader.read(null, jsonDecoder)))
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(record => Try(fromRecord(record)))

  def singleEntity: Try[T] = Try{dataumReader.read(null, jsonDecoder)}.map(fromRecord.apply)
}

object AvroInputStream {
  def apply[T: SchemaFor : FromRecord](bytes: Array[Byte]): AvroInputStream[T] = new AvroInputStream[T](new SeekableByteArrayInput(bytes))
  def apply[T: SchemaFor : FromRecord](path: String): AvroInputStream[T] = apply(new File(path))
  def apply[T: SchemaFor : FromRecord](path: Path): AvroInputStream[T] = apply(path.toFile)
  def apply[T: SchemaFor : FromRecord](file: File): AvroInputStream[T] = new AvroInputStream[T](new SeekableByteArrayInput(Files.readAllBytes(file.toPath)))
}