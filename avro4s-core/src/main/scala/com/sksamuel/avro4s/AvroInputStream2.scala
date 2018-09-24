package com.sksamuel.avro4s

import java.io.{File, InputStream}
import java.nio.file.{Path, Paths}

import com.sksamuel.avro4s.internal.Decoder
import org.apache.avro.Schema
import org.apache.avro.file.{SeekableByteArrayInput, SeekableFileInput}

import scala.util.Try

object AvroInputStream2 {

//  def binary[T](in: InputStream)
//               (implicit schemaEncoder: SchemaEncoder[T], decoder: Decoder[T]): AvroBinaryInputStream2[T] = binary[T](in, schemaEncoder.encode())
//
//  def binary[T](in: InputStream, writerSchema: Schema)
//               (implicit decoder: Decoder[T]): AvroBinaryInputStream2[T] = new AvroBinaryInputStream2[T](in, decoder, writerSchema, writerSchema)
//
//  def binary[T: SchemaEncoder : Decoder](bytes: Array[Byte]): AvroBinaryInputStream2[T] = binary[T](new SeekableByteArrayInput(bytes))
//  def binary[T: Decoder](bytes: Array[Byte], writerSchema: Schema): AvroBinaryInputStream2[T] = binary[T](new SeekableByteArrayInput(bytes), writerSchema)

  def binary[T: Decoder](file: File, writerSchema: Schema): AvroBinaryInputStream2[T] = binary(new SeekableFileInput(file), writerSchema)
  def binary[T: Decoder](path: String, writerSchema: Schema): AvroBinaryInputStream2[T] = binary(Paths.get(path), writerSchema)
  def binary[T: Decoder](path: Path, writerSchema: Schema): AvroBinaryInputStream2[T] = binary(path.toFile, writerSchema)
}

trait AvroInputStream2[T] {
  def close(): Unit
  def iterator: Iterator[T]
  def tryIterator: Iterator[Try[T]]
}
