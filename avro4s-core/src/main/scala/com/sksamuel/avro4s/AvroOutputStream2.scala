package com.sksamuel.avro4s

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import com.sksamuel.avro4s.internal.{Encoder, SchemaEncoder}
import org.apache.avro.file.CodecFactory

trait AvroOutputStream2[T] {
  def close(): Unit
  def flush(): Unit
  def fSync(): Unit
  def write(t: T): Unit
  def write(ts: Seq[T]): Unit = ts.foreach(write)
}

object AvroOutputStream2 {

  def json[T: SchemaFor : ToRecord](file: File): AvroJsonOutputStream[T] = json(file.toPath)
  def json[T: SchemaFor : ToRecord](path: Path): AvroJsonOutputStream[T] = json(Files.newOutputStream(path))
  def json[T: SchemaFor : ToRecord](os: OutputStream): AvroJsonOutputStream[T] = AvroJsonOutputStream(os)

  def data[T: SchemaFor : ToRecord](file: File, codec: CodecFactory): AvroDataOutputStream[T] = data(file.toPath, codec)
  def data[T: SchemaFor : ToRecord](path: Path, codec: CodecFactory): AvroDataOutputStream[T] = data(Files.newOutputStream(path), codec)
  def data[T: SchemaFor : ToRecord](os: OutputStream, codec: CodecFactory): AvroDataOutputStream[T] = AvroDataOutputStream(os, codec)

  // convenience api for cases where the the user want to use the default null codec.
  def data[T: SchemaFor : ToRecord](file: File): AvroDataOutputStream[T] = data(file.toPath)
  def data[T: SchemaFor : ToRecord](path: Path): AvroDataOutputStream[T] = data(Files.newOutputStream(path))
  def data[T: SchemaFor : ToRecord](os: OutputStream): AvroDataOutputStream[T] = AvroDataOutputStream(os)

  def binary[T: SchemaEncoder : Encoder](file: File): AvroBinaryOutputStream2[T] = binary(file.toPath)
  def binary[T: SchemaEncoder : Encoder](path: Path): AvroBinaryOutputStream2[T] = binary(Files.newOutputStream(path))
  def binary[T](os: OutputStream)(implicit schemaEncoder: SchemaEncoder[T], encoder: Encoder[T]): AvroBinaryOutputStream2[T] = AvroBinaryOutputStream2(os, schemaEncoder.encode(), encoder)
}