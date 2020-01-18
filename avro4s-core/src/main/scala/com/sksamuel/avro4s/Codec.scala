package com.sksamuel.avro4s

import org.apache.avro.Schema

import scala.annotation.implicitNotFound

@implicitNotFound(msg = """Unable to build a codec; please check the following:
- an implicit com.sksamuel.avro4s.FieldMapper must be in scope, and
- the given type must be either a case class or a sealed trait with subtypes being
  case classes or case objects.""")
trait Codec[T] extends EncoderV2[T] with DecoderV2[T] with SchemaAware[Codec, T] {
  self =>

  def schema: Schema

  def encode(value: T): AnyRef

  def decode(value: Any): T

  def withSchema(schemaFor: SchemaForV2[T], fieldMapper: FieldMapper = DefaultFieldMapper): Codec[T] = new Codec[T] {
    val schema: Schema = schemaFor.schema

    def encode(value: T): AnyRef = self.encode(value)

    def decode(value: Any): T = self.decode(value)
  }
}

object Codec
    extends MagnoliaGeneratedCodecs
    with ShapelessCoproductCodecs
    with BaseCodecs
    with StringCodecs
    with BigDecimalCodecs {

  implicit class CodecBifunctor[T](val codec: Codec[T]) extends AnyVal {
    def inmap[S](map: T => S, comap: S => T, f: Schema => Schema = identity): Codec[S] = {
      new Codec[S] {
        val schema: Schema = f(codec.schema)

        def encode(value: S): AnyRef = codec.encode(comap(value))

        def decode(value: Any): S = map(codec.decode(value))
      }
    }
  }
}
