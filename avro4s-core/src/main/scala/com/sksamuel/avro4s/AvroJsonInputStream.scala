package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.DecoderFactory

import java.io.InputStream
import scala.util.Try

final case class AvroJsonInputStream[T](in: InputStream,
                                        writerSchema: Schema)
                                       (using decoder: Decoder[T]) extends AvroInputStream[T] {

  private val datumReader = new GenericDatumReader[AnyRef](writerSchema)
  private val deserializer = DecoderFactory.get.jsonDecoder(writerSchema, in)
  private val decodeT = decoder.decode(writerSchema)

  private def next = Try {
    datumReader.read(null, deserializer)
  }

  def iterator: Iterator[T] = Iterator.continually(next)
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(decodeT.apply(_))

  def tryIterator: Iterator[Try[T]] = Iterator.continually(next)
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(record => Try(decodeT.apply(record)))

  def singleEntity: Try[T] = next.map(decodeT.apply(_))

  override def close(): Unit = in.close()
}
