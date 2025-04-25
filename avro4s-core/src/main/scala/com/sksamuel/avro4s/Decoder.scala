package com.sksamuel.avro4s

import com.sksamuel.avro4s.decoders.{ByteDecoders, CollectionDecoders, EitherDecoders, MagnoliaDerivedDecoder, OptionDecoders, PrimitiveDecoders, StringDecoders, TemporalDecoders}
import org.apache.avro.Schema

/**
  * A [[Decoder]] is used to convert an Avro value, such as a GenericRecord,
  * SpecificRecord, GenericFixed, EnumSymbol, or a basic type, into a
  * specified Scala type.
  *
  * For example, a Decoder[String] would convert an input into a plain Java String.
  *
  * Another example, a decoder for Option[String] would handle inputs of null
  * by emitting a None, and a non-null input by emitting a String wrapped in a Some.
  */
trait Decoder[T] extends Serializable {
  self =>

  def decode(schema: Schema): Any => T

  final def map[U](f: T => U): Decoder[U] = new Decoder[U] {
    override def decode(schema: Schema): Any => U = { input =>
      f(self.decode(schema).apply(input))
    }
  }
}

object Decoder
  extends PrimitiveDecoders
    with BigDecimalDecoders
    with ByteDecoders
    with CollectionDecoders
    with EitherDecoders
    with OptionDecoders
    with StringDecoders
    with TemporalDecoders
    with MagnoliaDerivedDecoder {
  def apply[T](using decoder: Decoder[T]): Decoder[T] = decoder
}