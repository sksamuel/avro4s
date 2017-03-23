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

class AvroBinaryInputStream[T](in: InputStream, writerSchema: org.apache.avro.Schema = null)
                              (implicit schemaFor: SchemaFor[T], fromRecord: FromRecord[T])
  extends AvroInputStream[T] {
  val wSchema = if (writerSchema == null) schemaFor() else writerSchema
  val datumReader = new GenericDatumReader[GenericRecord](wSchema, schemaFor())
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
    override def next(): T = fromRecord(result.next)
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = result.hasNext
    override def next(): Try[T] = Try(fromRecord(result.next))
  }

  override def close(): Unit = in.close()
}

class AvroDataInputStream[T](in: SeekableInput)
                            (implicit schemaFor: SchemaFor[T], fromRecord: FromRecord[T])
  extends AvroInputStream[T] {

  val datumReader = new GenericDatumReader[GenericRecord](schemaFor())
  val dataFileReader = new DataFileReader[GenericRecord](in, datumReader)

  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): T = fromRecord(dataFileReader.next)
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): Try[T] = Try(fromRecord(dataFileReader.next))
  }

  override def close(): Unit = in.close()
}

final case class AvroJsonInputStream[T](in: InputStream)
                                       (implicit schemaFor: SchemaFor[T], fromRecord: FromRecord[T])
  extends AvroInputStream[T] {

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

  override def close(): Unit = in.close()
}

object AvroInputStream {

  def json[T: SchemaFor : FromRecord](in: InputStream): AvroJsonInputStream[T] = new AvroJsonInputStream[T](in)
  def json[T: SchemaFor : FromRecord](bytes: Array[Byte]): AvroJsonInputStream[T] = json(new SeekableByteArrayInput(bytes))
  def json[T: SchemaFor : FromRecord](file: File): AvroJsonInputStream[T] = json(new SeekableFileInput(file))
  def json[T: SchemaFor : FromRecord](path: String): AvroJsonInputStream[T] = json(Paths.get(path))
  def json[T: SchemaFor : FromRecord](path: Path): AvroJsonInputStream[T] = json(path.toFile)

  def binary[T: SchemaFor : FromRecord](in: InputStream): AvroBinaryInputStream[T] = new AvroBinaryInputStream[T](in)
  def binary[T: SchemaFor : FromRecord](bytes: Array[Byte]): AvroBinaryInputStream[T] = binary(new SeekableByteArrayInput(bytes))
  def binary[T: SchemaFor : FromRecord](file: File): AvroBinaryInputStream[T] = binary(new SeekableFileInput(file))
  def binary[T: SchemaFor : FromRecord](path: String): AvroBinaryInputStream[T] = binary(Paths.get(path))
  def binary[T: SchemaFor : FromRecord](path: Path): AvroBinaryInputStream[T] = binary(path.toFile)

  def binaryEvolve[T: SchemaFor : FromRecord](in: InputStream, writerSchema: org.apache.avro.Schema): AvroBinaryInputStream[T] = new AvroBinaryInputStream[T](in, writerSchema)
  def binaryEvolve[T: SchemaFor : FromRecord](bytes: Array[Byte], writerSchema: org.apache.avro.Schema): AvroBinaryInputStream[T] = binaryEvolve(new SeekableByteArrayInput(bytes), writerSchema)
  def binaryEvolve[T: SchemaFor : FromRecord](file: File, writerSchema: org.apache.avro.Schema): AvroBinaryInputStream[T] = binaryEvolve(new SeekableFileInput(file), writerSchema)
  def binaryEvolve[T: SchemaFor : FromRecord](path: String, writerSchema: org.apache.avro.Schema): AvroBinaryInputStream[T] = binaryEvolve(Paths.get(path), writerSchema)
  def binaryEvolve[T: SchemaFor : FromRecord](path: Path, writerSchema: org.apache.avro.Schema): AvroBinaryInputStream[T] = binaryEvolve(path.toFile, writerSchema)

  def data[T: SchemaFor : FromRecord](bytes: Array[Byte]): AvroDataInputStream[T] = new AvroDataInputStream[T](new SeekableByteArrayInput(bytes))
  def data[T: SchemaFor : FromRecord](file: File): AvroDataInputStream[T] = new AvroDataInputStream[T](new SeekableFileInput(file))
  def data[T: SchemaFor : FromRecord](path: String): AvroDataInputStream[T] = data(Paths.get(path))
  def data[T: SchemaFor : FromRecord](path: Path): AvroDataInputStream[T] = data(path.toFile)

  @deprecated("Use .json .data or .binary to make it explicit which type of output you want", "1.5.0")
  def apply[T: SchemaFor : FromRecord](bytes: Array[Byte]): AvroInputStream[T] = new AvroBinaryInputStream[T](new SeekableByteArrayInput(bytes))
  @deprecated("Use .json .data or .binary to make it explicit which type of output you want", "1.5.0")
  def apply[T: SchemaFor : FromRecord](path: String): AvroInputStream[T] = apply(new File(path))
  @deprecated("Use .json .data or .binary to make it explicit which type of output you want", "1.5.0")
  def apply[T: SchemaFor : FromRecord](path: Path): AvroInputStream[T] = apply(path.toFile)
  @deprecated("Use .json .data or .binary to make it explicit which type of output you want", "1.5.0")
  def apply[T: SchemaFor : FromRecord](file: File): AvroInputStream[T] = new AvroDataInputStream[T](new SeekableFileInput(file))
}