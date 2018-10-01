package com.sksamuel.avro4s

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path, Paths}

import com.sksamuel.avro4s.internal.Encoder
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
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
trait AvroOutputStream[T] {
  def close(): Unit
  def close(closeUnderlying: Boolean): Unit
  def flush(): Unit
  def fSync(): Unit
  def write(t: T): Unit
  def write(ts: Seq[T]): Unit = ts.foreach(write)
}


class DefaultAvroOutputStream[T](os: OutputStream, schema: Schema, serializer: org.apache.avro.io.Encoder)
                                (implicit encoder: Encoder[T]) extends AvroOutputStream[T] {

  private val datumWriter = new GenericDatumWriter[GenericRecord](schema)

  override def close(): Unit = close(true)
  override def close(closeUnderlying: Boolean): Unit = {
    flush()
    if (closeUnderlying)
      os.close()
  }

  override def write(t: T): Unit = {
    val record = encoder.encode(t, schema).asInstanceOf[GenericRecord]
    datumWriter.write(record, serializer)
  }

  override def flush(): Unit = serializer.flush()
  override def fSync(): Unit = ()
}

object AvroOutputStream {

  /**
    * An [[AvroOutputStream]] that does not write the [[org.apache.avro.Schema]]. Use this when
    * you want the smallest messages possible at the cost of not having the schema available
    * in the messages for downstream clients.
    */
  def binary[T: Encoder] = new AvroOutputStreamBuilder[T](BinaryFormat)

  def json[T: Encoder] = new AvroOutputStreamBuilder[T](JsonFormat)

  def data[T: Encoder] = new AvroOutputStreamBuilder[T](DataFormat)
}

class AvroOutputStreamBuilder[T: Encoder](format: AvroFormat) {
  def to(path: Path): AvroOutputStreamBuilderWithSource[T] = to(Files.newOutputStream(path))
  def to(path: String): AvroOutputStreamBuilderWithSource[T] = to(Paths.get(path))
  def to(file: File): AvroOutputStreamBuilderWithSource[T] = to(file.toPath)
  def to(out: OutputStream): AvroOutputStreamBuilderWithSource[T] = new AvroOutputStreamBuilderWithSource(format, out)
}

class AvroOutputStreamBuilderWithSource[T: Encoder](format: AvroFormat, out: OutputStream, codec: CodecFactory = CodecFactory.nullCodec) {

  def withCodec(codec: CodecFactory) = new AvroOutputStreamBuilderWithSource[T](format, out, codec)

  /**
    * Builds an [[AvroInputStream]] with the specified writer schema.
    */
  def build(schema: Schema) = format match {
    case DataFormat => new AvroDataOutputStream[T](out, schema, codec)
    case BinaryFormat => new DefaultAvroOutputStream[T](out, schema, EncoderFactory.get().binaryEncoder(out, null))
    case JsonFormat => new DefaultAvroOutputStream[T](out, schema, EncoderFactory.get().jsonEncoder(schema, out))
  }
}