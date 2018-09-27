package com.sksamuel.avro4s

import java.io.InputStream

import com.sksamuel.avro4s.internal.Decoder
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

import scala.util.Try

class AvroDataInputStream[T](in: InputStream,
                             writerSchema: Option[Schema],
                             readerSchema: Option[Schema])(implicit decoder: Decoder[T]) extends AvroInputStream[T] {

  // if no reader or writer schema is specified, then we create a reader that uses what's present in the files
  private val datumReader =
    if (writerSchema.isEmpty && readerSchema.isEmpty) new GenericDatumReader[GenericRecord]()
    else if (writerSchema.isDefined && readerSchema.isDefined) new GenericDatumReader[GenericRecord](writerSchema.get, readerSchema.get)
    else if (writerSchema.isDefined) new GenericDatumReader[GenericRecord](writerSchema.get)
    else new GenericDatumReader[GenericRecord](readerSchema.get)

  private val dataFileReader = new DataFileStream[GenericRecord](in, datumReader)

  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): T = decoder.decode(dataFileReader.next)
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): Try[T] = Try(decoder.decode(dataFileReader.next))
  }

  override def close(): Unit = in.close()
}
