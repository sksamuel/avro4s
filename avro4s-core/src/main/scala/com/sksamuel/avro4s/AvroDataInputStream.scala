package com.sksamuel.avro4s

import java.io.InputStream

import org.apache.avro.{AvroRuntimeException, Schema}
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.DatumReader

import scala.util.{Failure, Try}

class AvroDataInputStream[T](in: InputStream,
                             writerSchema: Option[Schema])
                            (implicit decoder: Decoder[T]) extends AvroInputStream[T] {


  // if no reader or writer schema is specified, then we create a reader that uses what's present in the files
  private val datumReader = writerSchema match {
    case Some(writer) => GenericData.get.createDatumReader(writer, decoder.schema)
    case None => GenericData.get.createDatumReader(null, decoder.schema)
  }

  private val dataFileReader = new DataFileStream[GenericRecord](in, datumReader.asInstanceOf[DatumReader[GenericRecord]])

  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): T = {
      val record = dataFileReader.next
      decoder.decode(record)
    }
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): Try[T] =
      Try(decoder.decode(dataFileReader.next)).recoverWith {
        case t: AvroRuntimeException =>
          dataFileReader.nextBlock() // in case of exception, skip to next block
          Failure(t)
      }
  }

  override def close(): Unit = in.close()
}
