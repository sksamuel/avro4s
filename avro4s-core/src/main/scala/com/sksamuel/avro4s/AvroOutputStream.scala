package com.sksamuel.avro4s

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import com.sksamuel.avro4s.internal.{Decoder, Encoder}
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
  *  - a BB stream
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
  def binary[T: Encoder](file: File, schema: Schema): AvroOutputStream[T] = binary(file.toPath, schema)
  def binary[T: Encoder](path: Path, schema: Schema): AvroOutputStream[T] = binary(Files.newOutputStream(path), schema)
  def binary[T: Encoder](os: OutputStream, schema: Schema): AvroOutputStream[T] = new DefaultAvroOutputStream(os, schema, EncoderFactory.get.binaryEncoder(os, null))

  // avro output stream that writes json instead of a packed format
  def json[T: Encoder](file: File, schema: Schema): AvroOutputStream[T] = json(file.toPath, schema)
  def json[T: Encoder](path: Path, schema: Schema): AvroOutputStream[T] = json(Files.newOutputStream(path), schema)
  def json[T: Encoder](os: OutputStream, schema: Schema): AvroOutputStream[T] = new DefaultAvroOutputStream(os, schema, EncoderFactory.get.jsonEncoder(schema, os, true))

  def data[T: Encoder](file: File, schema: Schema, codec: CodecFactory): AvroDataOutputStream[T] = data(file.toPath, schema, codec)
  def data[T: Encoder](path: Path, schema: Schema, codec: CodecFactory): AvroDataOutputStream[T] = data(Files.newOutputStream(path), schema, codec)
  def data[T: Encoder](os: OutputStream, schema: Schema, codec: CodecFactory): AvroDataOutputStream[T] = AvroDataOutputStream(os, schema, codec)

  def data[T: Encoder](file: File, schema: Schema): AvroDataOutputStream[T] = data(file.toPath, schema)
  def data[T: Encoder](path: Path, schema: Schema): AvroDataOutputStream[T] = data(Files.newOutputStream(path), schema)
  def data[T: Encoder](os: OutputStream, schema: Schema): AvroDataOutputStream[T] = AvroDataOutputStream(os, schema)
}