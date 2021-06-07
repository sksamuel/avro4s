package com.sksamuel.avro4s

import java.io.{ByteArrayInputStream, File, InputStream}
import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths}

import org.apache.avro.Schema

import scala.util.Try

trait AvroInputStream[T] extends AutoCloseable {

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
  def binary[T: Decoder]: AvroInputStreamBuilder[T] = new AvroInputStreamBuilder[T](AvroFormat.Binary)

  /**
    * Creates a new [[AvroInputStreamBuilder]] that will read from binary
    * encoded files with the schema present.
    */
  def data[T: Decoder]: AvroInputStreamBuilder[T] = new AvroInputStreamBuilder[T](AvroFormat.Data)

  /**
    * Creates a new [[AvroInputStreamBuilder]] that will read from json
    * encoded files.
    */
  def json[T: Decoder]: AvroInputStreamBuilder[T] = new AvroInputStreamBuilder[T](AvroFormat.Json)
}


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
    case AvroFormat.Data => new AvroDataInputStream[T](in, Some(writerSchema))
    case AvroFormat.Binary => new AvroBinaryInputStream[T](in, writerSchema)
    case AvroFormat.Json => new AvroJsonInputStream[T](in, writerSchema)
  }

  /**
    * Builds an [[AvroInputStream]] that uses the schema present in the file.
    * This method does not work for binary or json formats because those
    * formats do not store the schema.
    */
  def build = format match {
    case AvroFormat.Data => new AvroDataInputStream[T](in, None)
    case _ => throw new Avro4sConfigurationException("Must specify a schema for binary or json formats")
  }
}
