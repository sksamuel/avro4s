package com.sksamuel.avro4s

import org.apache.avro.Schema

trait EncoderV2[T] extends SchemaAware[EncoderV2, T] {
  def schema: Schema

  def encode(value: T): AnyRef

  def withSchema(schemaFor: SchemaForV2[T], fieldMapper: FieldMapper = DefaultFieldMapper): EncoderV2[T]
}

object EncoderV2 {
  implicit class EncoderCofunctor[T](val encoder: EncoderV2[T]) extends AnyVal {
    def comap[S](f: S => T, sf: Schema => Schema = identity): EncoderV2[S] = {
      new EncoderV2[S] {
        val schema: Schema = sf(encoder.schema)

        def encode(value: S): AnyRef = encoder.encode(f(value))

        def withSchema(schemaFor: SchemaForV2[S], fieldMapper: FieldMapper): EncoderV2[S] = this
      }
    }
  }

  def apply[T](implicit codec: Codec[T]): EncoderV2[T] = codec


}
