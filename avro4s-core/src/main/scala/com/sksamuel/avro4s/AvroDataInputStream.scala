package com.sksamuel.avro4s

import java.io.InputStream

import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

import scala.util.Try

class AvroDataInputStream[T](in: InputStream,
                             writerSchema: Option[Schema])
                            (implicit decoder: Decoder[T]) extends AvroInputStream[T] {


  private val dataFileReader = {
    // if no reader or writer schema is specified, then we create a reader that uses what's present in the files
    val datumReader = new GenericDatumReader[GenericRecord](writerSchema.orNull, decoder.schema)
    new DataFileStream[GenericRecord](in, datumReader)
  }

  override def iterator: Iterator[T] = new Iterator[T] {
    var record: GenericRecord = null
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): T = {
      record = dataFileReader.next(record)
      decoder.decode(record)
    }
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    var record: GenericRecord = null
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): Try[T] = Try {
      record = dataFileReader.next(record)
      decoder.decode(record)
    }
  }

  override def close(): Unit = in.close()
}
