package com.sksamuel.avro4s

import java.io.{EOFException, InputStream}

import com.sksamuel.avro4s.internal.Decoder
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
  * input stream requires an instance of [[Decoder]].
  */
class AvroBinaryInputStream2[T](in: InputStream,
                                recordDecoder: Decoder[T],
                                writerSchema: Schema,
                                readerSchema: Schema) extends AvroInputStream[T] {

  private val datumReader = new GenericDatumReader[GenericRecord](writerSchema, readerSchema)
  private val avroBinaryDecoder = DecoderFactory.get().binaryDecoder(in, null)

  private val _iter = Iterator.continually {
    try {
      datumReader.read(null, avroBinaryDecoder)
    } catch {
      case _: EOFException => null
    }
  }.takeWhile(_ != null)

  /**
    * Returns an iterator for the values of T in the stream.
    */
  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = _iter.hasNext
    override def next(): T = recordDecoder.decode(_iter.next)
  }

  /**
    * Returns an iterator for values of Try[T], so that any
    * decoding issues are wrapped.
    */
  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = _iter.hasNext
    override def next(): Try[T] = Try(recordDecoder.decode(_iter.next))
  }

  override def close(): Unit = in.close()
}