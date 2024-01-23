package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.{DatumReader, DecoderFactory}

import java.io.InputStream
import scala.util.Try

/**
  * An implementation of [[AvroInputStream]] that reads values of type T
  * written as binary data.
  *
  * In avro, binary encodings do not include the schema. Therefore, this
  * input stream requires that the user provide the schema.
  *
  * A [[Decoder]] must be provided (usually implicitly) that will marshall
  * avro records into instances of type T.
  *
  * @param in           the input stream to read from
  * @param writerSchema the schema that was used to write the data
  * @param decoder      a mapping from the base avro type to an instance of T
  * @see https://avro.apache.org/docs/current/spec.html#binary_encoding
  */
class AvroBinaryInputStream[T](in: InputStream,
                               writerSchema: Schema,
                               readerSchema: Schema)
                              (using decoder: Decoder[T]) extends AvroInputStream[T] {

  def this(in: InputStream, writerSchema: Schema)(using decoder: Decoder[T]) = this(in, writerSchema, writerSchema)

  private val datumReader = new GenericDatumReader[Any](writerSchema, readerSchema, GenericData.get)
  private val avroDecoder = DecoderFactory.get().binaryDecoder(in, null)
  private val decodeT = decoder.decode(readerSchema)

  private val _iter = new Iterator[Any] {
    override def hasNext: Boolean = !avroDecoder.isEnd
    override def next(): Any = datumReader.read(null, avroDecoder)
  }

  /**
    * Returns an iterator for the values of T in the stream.
    */
  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = _iter.hasNext
    override def next(): T = decodeT(_iter.next())
  }

  /**
    * Returns an iterator for values of Try[T], so that any
    * decoding issues are wrapped.
    */
  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    var last: Option[Try[T]] = None

    override def hasNext: Boolean = _iter.hasNext && last.fold(true)(_.isSuccess)

    override def next(): Try[T] = {
      val next = Try(decodeT(_iter.next()))
      last = Option(next)
      next
    }
  }

  override def close(): Unit = in.close()
}
