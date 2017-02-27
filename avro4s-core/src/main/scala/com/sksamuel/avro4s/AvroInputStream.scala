package com.sksamuel.avro4s

import java.io.{EOFException, File, InputStream}
import java.nio.file.{Path, Paths}

import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput, SeekableFileInput, SeekableInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

import scala.util.Try

trait AvroInputStream[T] {
  def close(): Unit
  def iterator(): Iterator[T]
  def tryIterator(): Iterator[Try[T]]
}

class AvroBinaryInputStream[T](in: InputStream)
                              (implicit schemaFor: SchemaFor[T], fromValue: FromValue[T])
  extends AvroInputStream[T] {

  val datumReader = new GenericDatumReader[GenericRecord](schemaFor())
  val binDecoder = DecoderFactory.get().binaryDecoder(in, null)
  val records = new java.util.ArrayList[GenericRecord]()

  try {
    while (true)
      records.add(datumReader.read(null, binDecoder))
  } catch {
    case _: EOFException => null
  }
  val result = records.iterator()

  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = result.hasNext
    override def next(): T = fromValue(result.next)
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = result.hasNext
    override def next(): Try[T] = Try(fromValue(result.next))
  }

  override def close(): Unit = in.close()
}

class AvroDataInputStream[T](in: SeekableInput)
                            (implicit schemaFor: SchemaFor[T], fromValue: FromValue[T])
  extends AvroInputStream[T] {

  val datumReader = new GenericDatumReader[GenericRecord](schemaFor())
  val dataFileReader = new DataFileReader[GenericRecord](in, datumReader)

  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): T = fromValue(dataFileReader.next)
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): Try[T] = Try(fromValue(dataFileReader.next))
  }

  override def close(): Unit = in.close()
}

final case class AvroJsonInputStream[T](in: InputStream)
                                       (implicit schemaFor: SchemaFor[T], fromValue: FromValue[T])
  extends AvroInputStream[T] {

  private val schema = schemaFor()
  private val dataumReader = new GenericDatumReader[Any](schema)
  private val jsonDecoder = DecoderFactory.get.jsonDecoder(schema, in)

  def iterator: Iterator[T] = Iterator.continually(Try{dataumReader.read(null, jsonDecoder)})
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(v => fromValue(v))

  def tryIterator: Iterator[Try[T]] = Iterator.continually(Try(dataumReader.read(null, jsonDecoder)))
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(record => Try(fromValue(record)))

  def singleEntity: Try[T] = Try{dataumReader.read(null, jsonDecoder)}.map(v => fromValue(v))

  override def close(): Unit = in.close()
}

object AvroInputStream {

  def json[T: SchemaFor : FromValue](in: InputStream): AvroJsonInputStream[T] = new AvroJsonInputStream[T](in)
  def json[T: SchemaFor : FromValue](bytes: Array[Byte]): AvroJsonInputStream[T] = json(new SeekableByteArrayInput(bytes))
  def json[T: SchemaFor : FromValue](file: File): AvroJsonInputStream[T] = json(new SeekableFileInput(file))
  def json[T: SchemaFor : FromValue](path: String): AvroJsonInputStream[T] = json(Paths.get(path))
  def json[T: SchemaFor : FromValue](path: Path): AvroJsonInputStream[T] = json(path.toFile)

  def binary[T: SchemaFor : FromValue](in: InputStream): AvroBinaryInputStream[T] = new AvroBinaryInputStream[T](in)
  def binary[T: SchemaFor : FromValue](bytes: Array[Byte]): AvroBinaryInputStream[T] = binary(new SeekableByteArrayInput(bytes))
  def binary[T: SchemaFor : FromValue](file: File): AvroBinaryInputStream[T] = binary(new SeekableFileInput(file))
  def binary[T: SchemaFor : FromValue](path: String): AvroBinaryInputStream[T] = binary(Paths.get(path))
  def binary[T: SchemaFor : FromValue](path: Path): AvroBinaryInputStream[T] = binary(path.toFile)

  def data[T: SchemaFor : FromValue](bytes: Array[Byte]): AvroDataInputStream[T] = new AvroDataInputStream[T](new SeekableByteArrayInput(bytes))
  def data[T: SchemaFor : FromValue](file: File): AvroDataInputStream[T] = new AvroDataInputStream[T](new SeekableFileInput(file))
  def data[T: SchemaFor : FromValue](path: String): AvroDataInputStream[T] = data(Paths.get(path))
  def data[T: SchemaFor : FromValue](path: Path): AvroDataInputStream[T] = data(path.toFile)

  @deprecated("Use .json .data or .binary to make it explicit which type of output you want", "1.5.0")
  def apply[T: SchemaFor : FromValue](bytes: Array[Byte]): AvroInputStream[T] = new AvroBinaryInputStream[T](new SeekableByteArrayInput(bytes))
  @deprecated("Use .json .data or .binary to make it explicit which type of output you want", "1.5.0")
  def apply[T: SchemaFor : FromValue](path: String): AvroInputStream[T] = apply(new File(path))
  @deprecated("Use .json .data or .binary to make it explicit which type of output you want", "1.5.0")
  def apply[T: SchemaFor : FromValue](path: Path): AvroInputStream[T] = apply(path.toFile)
  @deprecated("Use .json .data or .binary to make it explicit which type of output you want", "1.5.0")
  def apply[T: SchemaFor : FromValue](file: File): AvroInputStream[T] = new AvroDataInputStream[T](new SeekableFileInput(file))
}