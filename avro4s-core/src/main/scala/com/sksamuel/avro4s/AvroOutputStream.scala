package com.sksamuel.avro4s

import org.apache.avro.Schema

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path, Paths}
import org.apache.avro.file.CodecFactory
import org.apache.avro.io.EncoderFactory

/**
  * An [[AvroOutputStream]] will write instances of T to an underlying
  * representation.
  *
  * There are three implementations of this stream
  *  - a Data stream,
  *  - a Binary stream
  *  - a Json stream
  *
  * See the methods on the companion object to create instances of each
  * of these types of stream.
  */
trait AvroOutputStream[T] extends AutoCloseable {
  def close(): Unit
  def flush(): Unit
  def fSync(): Unit
  def write(t: T): Unit
  def write(ts: Seq[T]): Unit = ts.foreach(write)
}

object AvroOutputStream:

  /**
    * An [[AvroOutputStream]] that does not write the schema.
    *
    * Use this when you want the smallest messages possible at the cost of not having the
    * schema available in the messages for downstream clients.
    */
  //  def binary[T](schema: Schema, encoder: Encoder[T]): AvroOutputStreamBuilder[T] =
  //    given Encoder[T] = encoder
  //    new AvroOutputStreamBuilder[T](schema, AvroFormat.Binary)

  def binary[T](using schemaFor: SchemaFor[T], encoder: Encoder[T]): AvroOutputStreamBuilder[T] =
    new AvroOutputStreamBuilder[T](schemaFor.schema, encoder, AvroFormat.Binary)

  /**
    * An [[AvroOutputStream]] that writes as JSON.
    */
  //  def json[T: Encoder](schema: Schema): AvroOutputStreamBuilder[T] =
  //    new AvroOutputStreamBuilder[T](schema, AvroFormat.Json)

  def json[T](using schemaFor: SchemaFor[T], encoder: Encoder[T]): AvroOutputStreamBuilder[T] =
    new AvroOutputStreamBuilder[T](schemaFor.schema, encoder, AvroFormat.Json)

  /**
    * An [[AvroOutputStream]] that writes the schema alongside data.
    *
    * This is the standard implementation for Avro.
    */
  def data[T](schema: Schema, encoder: Encoder[T]): AvroOutputStreamBuilder[T] =
    new AvroOutputStreamBuilder[T](schema, encoder, AvroFormat.Data)

  def data[T](using schemaFor: SchemaFor[T], encoder: Encoder[T]): AvroOutputStreamBuilder[T] =
    new AvroOutputStreamBuilder[T](schemaFor.schema, encoder, AvroFormat.Data)

class AvroOutputStreamBuilder[T](schema: Schema, encoder: Encoder[T], format: AvroFormat) {
  def to(path: Path): AvroOutputStreamBuilderWithSource[T] = to(Files.newOutputStream(path))
  def to(path: String): AvroOutputStreamBuilderWithSource[T] = to(Paths.get(path))
  def to(file: File): AvroOutputStreamBuilderWithSource[T] = to(file.toPath)
  def to(out: OutputStream): AvroOutputStreamBuilderWithSource[T] =
    new AvroOutputStreamBuilderWithSource(schema, encoder, format, out)
}

case class AvroOutputStreamBuilderWithSource[T](schema: Schema,
                                                encoder: Encoder[T],
                                                format: AvroFormat,
                                                out: OutputStream,
                                                codec: CodecFactory = CodecFactory.nullCodec) {

  def withCodec(codec: CodecFactory) = copy(codec = codec)

  /**
    * Builds an [[AvroOutputStream]]
    */
  def build(): AvroOutputStream[T] = {
    given Encoder[T] = encoder
    format match {
      case AvroFormat.Data => new AvroDataOutputStream[T](schema, out, codec)
      case AvroFormat.Binary => new AvroBinaryOutputStream[T](schema, out, EncoderFactory.get().binaryEncoder(out, null))
      case AvroFormat.Json =>
        val serializer = EncoderFactory.get().jsonEncoder(schema, out)
        new AvroBinaryOutputStream[T](schema, out, serializer)
    }
  }
}
