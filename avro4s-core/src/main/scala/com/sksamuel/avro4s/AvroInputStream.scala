package com.sksamuel.avro4s

import java.io.{ByteArrayInputStream, File, InputStream}
import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths}

import com.sksamuel.avro4s.internal.Decoder
import org.apache.avro.Schema

import scala.util.Try

trait AvroInputStream[T] {

  /**
    * Closes this stream and any underlying resources.
    */
  def close(): Unit

  /**
    * Returns an iterator for the values of T in the stream.
    */
  def iterator: Iterator[T]

  /**
    * Returns an iterator for values of Try[T], so that any
    * decoding issues are wrapped.
    */
  def tryIterator: Iterator[Try[T]]
}

object AvroInputStream {

  /**
    * Creates a new [[AvroInputStreamBuilder]] that will read from binary
    * encoded files.
    */
  def binary[T: Decoder]: AvroInputStreamBuilder[T] = new AvroInputStreamBuilder[T](BinaryFormat)

  /**
    * Creates a new [[AvroInputStreamBuilder]] that will read from binary
    * encoded files with the schema present.
    */
  def data[T: Decoder]: AvroInputStreamBuilder[T] = new AvroInputStreamBuilder[T](DataFormat)

    /**
    * Creates a new [[AvroInputStreamBuilder]] that will read from json
    * encoded files.
    */
  def json[T: Decoder]: AvroInputStreamBuilder[T] = new AvroInputStreamBuilder[T](JsonFormat)
}

sealed trait AvroFormat
object BinaryFormat extends AvroFormat
object JsonFormat extends AvroFormat
object DataFormat extends AvroFormat

class AvroInputStreamBuilder[T: Decoder](format: AvroFormat) {
  def from(path: Path): AvroInputStreamBuilderWithSource[T] = from(Files.newInputStream(path))
  def from(path: String): AvroInputStreamBuilderWithSource[T] = from(Paths.get(path))
  def from(file: File): AvroInputStreamBuilderWithSource[T] = from(file.toPath)
  def from(in: InputStream): AvroInputStreamBuilderWithSource[T] = new AvroInputStreamBuilderWithSource(format, in)
  def from(bytes: Array[Byte]): AvroInputStreamBuilderWithSource[T] = from(new ByteArrayInputStream(bytes))
  def from(buffer: ByteBuffer): AvroInputStreamBuilderWithSource[T] = from(new ByteArrayInputStream(buffer.array))
}

class AvroInputStreamBuilderWithSource[T: Decoder](format: AvroFormat, in: InputStream) {

  /**
    * Builds an [[AvroInputStream]] with the specified writer schema.
    */
  def build(writerSchema: Schema) = format match {
    case DataFormat => new AvroDataInputStream[T](in, Some(writerSchema), None)
    case BinaryFormat => new AvroBinaryInputStream[T](in, writerSchema, writerSchema)
    case JsonFormat => new AvroJsonInputStream[T](in, writerSchema, writerSchema)
  }

  /**
    * Builds an [[AvroInputStream]] with the specified reader and
    * write schemas.
    */
  def build(writerSchema: Schema, readerSchema: Schema) = format match {
    case DataFormat => new AvroDataInputStream[T](in, Some(writerSchema), Some(readerSchema))
    case BinaryFormat => new AvroBinaryInputStream[T](in, writerSchema, readerSchema)
    case JsonFormat => new AvroJsonInputStream[T](in, writerSchema, readerSchema)
  }

  /**
    * Builds an [[AvroInputStream]] that uses the schema present in the file.
    * This method does not work for binary or json formats because those
    * formats do not store the schema.
    */
  def build = format match {
    case DataFormat => new AvroDataInputStream[T](in, None, None)
    case _ => sys.error("Must specify a schema for binary or json formats")
  }
}