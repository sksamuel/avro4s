package com.sksamuel.avro4s

import java.io.InputStream

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory

import scala.util.Try

final case class AvroJsonInputStream[T](in: InputStream,
                                        writerSchema: Schema,
                                        readerSchema: Schema)
                                       (implicit decoder: Decoder[T]) extends AvroInputStream[T] {

  private val datumReader = new DefaultAwareDatumReader[GenericRecord](writerSchema, readerSchema)
  private val jsonDecoder = DecoderFactory.get.jsonDecoder(writerSchema, in)

  private def next = Try {
    datumReader.read(null, jsonDecoder)
  }

  def iterator: Iterator[T] = Iterator.continually(next)
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(decoder.decode(_, readerSchema))

  def tryIterator: Iterator[Try[T]] = Iterator.continually(next)
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(record => Try(decoder.decode(record, readerSchema)))

  def singleEntity: Try[T] = next.map(decoder.decode(_, readerSchema))

  override def close(): Unit = in.close()
}
