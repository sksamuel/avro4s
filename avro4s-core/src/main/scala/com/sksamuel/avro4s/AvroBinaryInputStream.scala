package com.sksamuel.avro4s

import java.io.InputStream

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

import scala.util.Try

/**
  * An implementation of [[AvroInputStream]] that reads values of type T
  * written as binary data.
  * See https://avro.apache.org/docs/current/spec.html#binary_encoding
  *
  * In order to convert the underlying binary data into types of T, this
  * input stream requires an instance of Decoder.
  */
class AvroBinaryInputStream[T](in: InputStream,
                               writerSchema: Schema)
                              (implicit decoder: Decoder[T]) extends AvroInputStream[T] {

  private val datumReader = new GenericDatumReader[GenericRecord](writerSchema,  decoder.schema)
  private val avroDecoder = DecoderFactory.get().binaryDecoder(in, null)

  private val _iter = new Iterator[GenericRecord] {
    override def hasNext: Boolean = !avroDecoder.isEnd
    override def next(): GenericRecord = datumReader.read(null, avroDecoder)
  }

  /**
    * Returns an iterator for the values of T in the stream.
    */
  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = _iter.hasNext
    override def next(): T = decoder.decode(_iter.next())
  }

  /**
    * Returns an iterator for values of Try[T], so that any
    * decoding issues are wrapped.
    */
  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    var last: Option[Try[T]] = None

    override def hasNext: Boolean = _iter.hasNext && last.fold(true)(_.isSuccess)

    override def next(): Try[T] = {
      val next = Try(decoder.decode(_iter.next()))
      last = Option(next)
      next
    }
  }

  override def close(): Unit = in.close()
}
