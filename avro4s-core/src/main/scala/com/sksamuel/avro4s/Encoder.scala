package com.sksamuel.avro4s

import org.apache.avro.Schema

trait Encoder[T] extends SchemaAware[Encoder, T] with Serializable { self =>

  def schema: Schema

  def encode(value: T): AnyRef

  def withSchema(schemaFor: SchemaForV2[T]): Encoder[T] = {
    val sf = schemaFor
    new Encoder[T] {
      val schemaFor: SchemaForV2[T] = sf

      def encode(value: T): AnyRef = self.encode(value)
    }
  }

}

object Encoder
    extends MagnoliaGeneratedEncoders
    with ShapelessCoproductEncoders
    with ScalaPredefAndCollectionEncoders
    with BigDecimalEncoders
    with ByteIterableEncoders
    with TemporalEncoders
    with BaseEncoders {

  def apply[T](implicit encoder: Encoder[T]): Encoder[T] = encoder

  private class DelegatingEncoder[T, S](encoder: Encoder[T], val schemaFor: SchemaForV2[S], comap: S => T) extends Encoder[S] {

    def encode(value: S): AnyRef = encoder.encode(comap(value))

    override def withSchema(schemaFor: SchemaForV2[S]): Encoder[S] = {
      // pass through decoder transformation.
      val decoderWithSchema = encoder.withSchema(schemaFor.forType)
      new DelegatingEncoder[T, S](decoderWithSchema, schemaFor.forType, comap)
    }
  }

  implicit class EncoderCofunctor[T](val encoder: Encoder[T]) extends AnyVal {
    def comap[S](f: S => T): Encoder[S] = new DelegatingEncoder(encoder, encoder.schemaFor.forType, f)
  }
}
