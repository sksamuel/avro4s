package com.sksamuel.avro4s

import java.io.InputStream

import org.apache.avro.{AvroRuntimeException, Schema}
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericData
import org.apache.avro.io.DatumReader

import scala.util.{Failure, Try}

/**
  * An implementation of [[AvroInputStream]] that reads values of type T
  * written as Avro.
  *
  * Avro data files contain the schema as part of the message payload. Therefore, no schema
  * is necessarily required to read the data back and the decoder will use the schema
  * present in the payload. However, for efficiency, if the schema is provided, then a
  * decoder can be pre-built and used on each contained object.
  *
  * A [[Decoder]] must be provided (usually implicitly) that will marshall
  * avro records into instances of type T.
  *
  * @param in           the input stream to read from
  * @param writerSchema the schema that was used to write the data. Optional, but if provided will
  *                     allow the decoder to be re-used for every contained object.
  * @param decoder      a mapping from the base avro type to an instance of T
  */
class AvroDataInputStream[T](in: InputStream,
                             writerSchema: Option[Schema])
                            (using decoder: Decoder[T]) extends AvroInputStream[T] {

  // if no writer schema is specified, then we create a reader that uses what's present in the files
  private val datumReader: DatumReader[Any] = writerSchema match {
    case Some(schema) => GenericData.get.createDatumReader(schema).asInstanceOf[DatumReader[Any]]
    case _ => GenericData.get.createDatumReader(null).asInstanceOf[DatumReader[Any]]
  }

  private val dataFileReader = new DataFileStream[Any](in, datumReader)
  private val decodeT = writerSchema.map(schema => decoder.decode(schema))

  private def decode(record: Any, schema: Schema) = decodeT.getOrElse(decoder.decode(schema)).apply(record)

  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): T = {
      val record = dataFileReader.next
      decode(record, dataFileReader.getSchema)
    }
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): Try[T] =
      Try(decode(dataFileReader.next, dataFileReader.getSchema)).recoverWith {
        case t: AvroRuntimeException =>
          dataFileReader.nextBlock() // in case of exception, skip to next block
          Failure(t)
      }
  }

  override def close(): Unit = in.close()
}
