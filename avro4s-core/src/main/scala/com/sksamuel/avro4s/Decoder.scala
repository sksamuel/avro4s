package com.sksamuel.avro4s

import com.sksamuel.avro4s.decoders.{CollectionDecoders, EitherDecoders, MagnoliaDerivedDecoder, PrimitiveDecoders, StringDecoders, TemporalDecoders}
import org.apache.avro.Schema

///**
//  * A [[Decoder]] is used to convert an Avro value, such as a GenericRecord,
//  * SpecificRecord, GenericFixed, EnumSymbol, or a basic JVM type, into a
//  * target Scala type.
//  *
//  * For example, a Decoder[String] would convert an input of type Utf8 -
//  * which is one of the ways Avro can encode strings - into a plain Java String.
//  *
//  * Another example, a decoder for Option[String] would handle inputs of null
//  * by emitting a None, and a non-null input by emitting the decoded value
//  * wrapped in a Some.
//  *
//  * A final example is converting a GenericData.Array or a Java collection type
//  * into a Scala collection type.
//  */
trait Decoder[T] {
  self =>

  def decode(schema: Schema): Any => T

  final def map[U](f: T => U): Decoder[U] = new Decoder[U] {
    override def decode(schema: Schema): Any => U = { input =>
      f(self.decode(schema).apply(input))
    }
  }
}

object Decoder
  extends StringDecoders
    with PrimitiveDecoders
    with EitherDecoders
    with CollectionDecoders
    with TemporalDecoders
    with MagnoliaDerivedDecoder {
  def apply[T](using decoder: Decoder[T]): Decoder[T] = decoder
}

trait BasicDecoder[T] extends Decoder[T] :
  def decode(value: Any): T
  override def decode(schema: Schema): Any => T = { value => decode(value) }
