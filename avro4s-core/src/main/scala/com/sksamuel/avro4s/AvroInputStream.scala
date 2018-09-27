package com.sksamuel.avro4s

import java.io.{ByteArrayInputStream, File, InputStream}
import java.nio.file.{Files, Path, Paths}

import com.sksamuel.avro4s.internal.Decoder
import org.apache.avro.Schema
import org.apache.avro.file.{SeekableByteArrayInput, SeekableFileInput}

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

  def binary[T: Decoder](path: String, schema: Schema): AvroBinaryInputStream[T] = binary(Paths.get(path), schema)
  def binary[T: Decoder](file: File, writerSchema: Schema): AvroBinaryInputStream[T] = binary(new SeekableFileInput(file), writerSchema)
  def binary[T: Decoder](path: Path, writerSchema: Schema): AvroBinaryInputStream[T] = binary(path.toFile, writerSchema)
  def binary[T: Decoder](bytes: Array[Byte], schema: Schema): AvroBinaryInputStream[T] = binary(new SeekableByteArrayInput(bytes), schema)
  def binary[T: Decoder](in: InputStream, schema: Schema): AvroBinaryInputStream[T] =
    new AvroBinaryInputStream[T](in, schema, schema)

  def data[T: Decoder](path: String, schema: Schema): AvroDataInputStream[T] = data(Paths.get(path), schema)
  def data[T: Decoder](file: File, writerSchema: Schema): AvroDataInputStream[T] = data(file.toPath, writerSchema)
  def data[T: Decoder](path: Path, writerSchema: Schema): AvroDataInputStream[T] = data(Files.newInputStream(path), writerSchema)
  def data[T: Decoder](bytes: Array[Byte], schema: Schema): AvroDataInputStream[T] = data(new ByteArrayInputStream(bytes), schema)
  def data[T: Decoder](in: InputStream, schema: Schema): AvroDataInputStream[T] =
    new AvroDataInputStream[T](in, Some(schema), Some(schema))
}

//object AvroInputStream {
//
//  def json[T: SchemaFor : FromRecord](in: InputStream): AvroJsonInputStream[T] = new AvroJsonInputStream[T](in)
//  def json[T: SchemaFor : FromRecord](bytes: Array[Byte]): AvroJsonInputStream[T] = json(new SeekableByteArrayInput(bytes))
//  def json[T: SchemaFor : FromRecord](file: File): AvroJsonInputStream[T] = json(new SeekableFileInput(file))
//  def json[T: SchemaFor : FromRecord](path: String): AvroJsonInputStream[T] = json(Paths.get(path))
//  def json[T: SchemaFor : FromRecord](path: Path): AvroJsonInputStream[T] = json(path.toFile)
//
//  def binary[T: SchemaFor : FromRecord](in: InputStream, writerSchema: Schema): AvroBinaryInputStream[T] = new AvroBinaryInputStream[T](in, Option(writerSchema))
//  def binary[T: SchemaFor : FromRecord](bytes: Array[Byte], writerSchema: Schema): AvroBinaryInputStream[T] = binary(new SeekableByteArrayInput(bytes), writerSchema)
//  def binary[T: SchemaFor : FromRecord](file: File, writerSchema: Schema): AvroBinaryInputStream[T] = binary(new SeekableFileInput(file), writerSchema)
//  def binary[T: SchemaFor : FromRecord](path: String, writerSchema: Schema): AvroBinaryInputStream[T] = binary(Paths.get(path), writerSchema)
//  def binary[T: SchemaFor : FromRecord](path: Path, writerSchema: Schema): AvroBinaryInputStream[T] = binary(path.toFile, writerSchema)
//
//  // convenience api for cases where the writer schema should be the same as the reader.
//  def binary[T: SchemaFor : FromRecord](in: InputStream): AvroBinaryInputStream[T] = new AvroBinaryInputStream[T](in)
//  def binary[T: SchemaFor : FromRecord](bytes: Array[Byte]): AvroBinaryInputStream[T] = binary(new SeekableByteArrayInput(bytes))
//  def binary[T: SchemaFor : FromRecord](file: File): AvroBinaryInputStream[T] = binary(new SeekableFileInput(file))
//  def binary[T: SchemaFor : FromRecord](path: String): AvroBinaryInputStream[T] = binary(Paths.get(path))
//  def binary[T: SchemaFor : FromRecord](path: Path): AvroBinaryInputStream[T] = binary(path.toFile)
//
//
//  sealed trait AvroFormat {
//    def newBuilder[T: SchemaFor : FromRecord](): AvroInputStreamBuilder[T]
//  }
//  object JsonFormat extends AvroFormat {
//    override def newBuilder[T: SchemaFor : FromRecord](): AvroInputStreamBuilder[T] = new AvroInputStreamBuilderJson[T]()
//  }
//  object BinaryFormat extends AvroFormat {
//    override def newBuilder[T: SchemaFor : FromRecord](): AvroInputStreamBuilder[T] = new AvroInputStreamBuilderBinary[T]()
//  }
//  object DataFormat extends AvroFormat {
//    override def newBuilder[T: SchemaFor : FromRecord](): AvroInputStreamBuilder[T] = new AvroInputStreamBuilderData[T]()
//  }
//
//  def builder[T: SchemaFor : FromRecord](format: AvroFormat): AvroInputStreamBuilder[T] = format.newBuilder[T]()
//
//  abstract class AvroInputStreamBuilder[T: SchemaFor : FromRecord] {
//    protected var writerSchema: Option[Schema] = None
//    protected var readerSchema: Option[Schema] = None
//    protected var inputStream: InputStream = _
//    protected var seekableInput: SeekableInput = _
//
//    def from(path: Path): this.type =
//      from(path.toFile)
//
//    def from(path: String): this.type =
//      from(Paths.get(path))
//
//    def from(file: File): this.type = {
//      val in = new SeekableFileInput(file)
//      this.inputStream = in
//      this.seekableInput = in
//      this
//    }
//
//    def from(bytes: Array[Byte]): this.type = {
//      val in = new SeekableByteArrayInput(bytes)
//      this.inputStream = in
//      this.seekableInput = in
//      this
//    }
//
//    def schema(writerSchema: Schema, readerSchema: Schema): this.type = {
//      this.writerSchema = Some(writerSchema)
//      this.readerSchema = Some(readerSchema)
//      this
//    }
//
//    def schema(schema: Schema): this.type =
//      this.schema(schema, schema)
//
//    def schemaWriterReader[Writer, Reader]()(implicit writerSchema: SchemaFor[Writer], readerSchema: SchemaFor[Reader]): this.type =
//      this.schema(writerSchema(), readerSchema())
//
//    def schema()(implicit schema: SchemaFor[T]): this.type =
//      this.schema(schema(), schema())
//
//    def build(): AvroInputStream[T]
//  }
//
//  trait FromInputStream {
//    this: AvroInputStreamBuilder[_] =>
//    def from(in: InputStream): this.type = {
//      this.inputStream = in
//      this
//    }
//  }
//
//  class AvroInputStreamBuilderJson[T: SchemaFor : FromRecord] extends AvroInputStreamBuilder[T] with FromInputStream {
//    override def build(): AvroInputStream[T] =
//      new AvroJsonInputStream[T](inputStream, writerSchema, readerSchema)
//  }
//  class AvroInputStreamBuilderBinary[T: SchemaFor : FromRecord] extends AvroInputStreamBuilder[T] with FromInputStream {
//    override def build(): AvroInputStream[T] =
//      new AvroBinaryInputStream[T](inputStream, writerSchema, readerSchema)
//  }
//  class AvroInputStreamBuilderData[T: SchemaFor : FromRecord] extends AvroInputStreamBuilder[T] {
//    override def build(): AvroInputStream[T] =
//      new AvroDataInputStream[T](seekableInput, writerSchema, readerSchema)
//  }
//}