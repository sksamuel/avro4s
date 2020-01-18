package com.sksamuel.avro4s

import org.apache.avro.Schema

trait DecoderV2[T] extends SchemaAware[DecoderV2, T] {

  def schema: Schema

  def decode(value: Any): T

  def withSchema(schemaFor: SchemaForV2[T]): DecoderV2[T]

}

object DecoderV2 extends ShapelessCoproductDecoders {
  implicit class DecoderFunctor[T](val decoder: DecoderV2[T]) extends AnyVal {
    def map[S](f: T => S, sf: Schema => Schema): DecoderV2[S] = {
      new DecoderV2[S] {
        val schema: Schema = sf(decoder.schema)

        def decode(value: Any): S = f(decoder.decode(value))

        def withSchema(schemaFor: SchemaForV2[S]): DecoderV2[S] = this
      }
    }
  }

  def apply[T](implicit codec: Codec[T]): DecoderV2[T] = codec
}
