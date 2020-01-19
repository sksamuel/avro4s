package com.sksamuel.avro4s

import org.apache.avro.Schema

trait EncoderV2[T] extends SchemaAware[EncoderV2, T] { self =>

  def schema: Schema

  def encode(value: T): AnyRef

  def withSchema(schemaFor: SchemaForV2[T]): EncoderV2[T] = new EncoderV2[T] {
    val schema: Schema = schemaFor.schema

    def encode(value: T): AnyRef = self.encode(value)
  }

}

object EncoderV2 extends ShapelessCoproductEncoders with BaseEncoders {

  private class DelegatingEncoder[T, S](encoder: EncoderV2[T], val schema: Schema, comap: S => T) extends EncoderV2[S] {

    def encode(value: S): AnyRef = encoder.encode(comap(value))

    override def withSchema(schemaFor: SchemaForV2[S]): EncoderV2[S] = {
      // pass through decoder transformation.
      val decoderWithSchema = encoder.withSchema(schemaFor.map(identity))
      new DelegatingEncoder[T, S](decoderWithSchema, schemaFor.schema, comap)
    }
  }

  implicit class EncoderCofunctor[T](val encoder: EncoderV2[T]) extends AnyVal {
    def comap[S](f: S => T): EncoderV2[S] = new DelegatingEncoder(encoder, encoder.schema, f)
  }

  def apply[T](implicit codec: Codec[T]): EncoderV2[T] = codec


}
