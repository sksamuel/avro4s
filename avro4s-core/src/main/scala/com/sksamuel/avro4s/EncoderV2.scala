package com.sksamuel.avro4s

import org.apache.avro.Schema

trait EncoderV2[T] extends SchemaAware[EncoderV2, T] with Serializable { self =>

  def schema: Schema

  def encode(value: T): AnyRef

  def withSchema(schemaFor: SchemaForV2[T]): EncoderV2[T] = {
    val sf = schemaFor
    new EncoderV2[T] {
      val schemaFor: SchemaForV2[T] = sf

      def encode(value: T): AnyRef = self.encode(value)
    }
  }

}

object EncoderV2
    extends MagnoliaGeneratedEncoders
    with ShapelessCoproductEncoders
    with ScalaPredefAndCollectionEncoders
    with BigDecimalEncoders
    with ByteIterableEncoders
    with TemporalEncoders
    with BaseEncoders {

  def apply[T](implicit encoder: EncoderV2[T]): EncoderV2[T] = encoder

  private class DelegatingEncoder[T, S](encoder: EncoderV2[T], val schemaFor: SchemaForV2[S], comap: S => T) extends EncoderV2[S] {

    def encode(value: S): AnyRef = encoder.encode(comap(value))

    override def withSchema(schemaFor: SchemaForV2[S]): EncoderV2[S] = {
      // pass through decoder transformation.
      val decoderWithSchema = encoder.withSchema(schemaFor.forType)
      new DelegatingEncoder[T, S](decoderWithSchema, schemaFor.forType, comap)
    }
  }

  implicit class EncoderCofunctor[T](val encoder: EncoderV2[T]) extends AnyVal {
    def comap[S](f: S => T): EncoderV2[S] = new DelegatingEncoder(encoder, encoder.schemaFor.forType, f)
  }
}
