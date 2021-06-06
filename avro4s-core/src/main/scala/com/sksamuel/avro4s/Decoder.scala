package com.sksamuel.avro4s

import com.sksamuel.avro4s.decoders.{CollectionDecoders, MagnoliaDerivedDecoder, PrimitiveDecoders, StringDecoders, TemporalDecoders}
import org.apache.avro.Schema

trait Decoder[T] {
  self =>

  def decode(schema: Schema): Any => T

  def map[U](f: T => U): Decoder[U] = new Decoder[U] {
    override def decode(schema: Schema): Any => U = { input =>
      f(self.decode(schema).apply(input))
    }
  }
}

object Decoder
  extends StringDecoders
    with PrimitiveDecoders
    with CollectionDecoders
    with TemporalDecoders
    with MagnoliaDerivedDecoder {
  def apply[T](using decoder: Decoder[T]): Decoder[T] = decoder
}

trait BasicDecoder[T] extends Decoder[T] :
  def decode(value: Any): T
  override def decode(schema: Schema): Any => T = { value => decode(value) }
