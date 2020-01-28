package com.sksamuel.avro4s

import org.apache.avro.Schema

trait Decoder[T] extends SchemaAware[Decoder, T] with Serializable { self =>

  def schemaFor: SchemaForV2[T]

  def schema: Schema

  def decode(value: Any): T

  def withSchema(schemaFor: SchemaForV2[T]): Decoder[T] = {
    val sf = schemaFor
    new Decoder[T] {
      def schemaFor: SchemaForV2[T] = sf
      def decode(value: Any): T = self.decode(value)
    }
  }
}

object Decoder
    extends MagnoliaGeneratedDecoders
    with ShapelessCoproductDecoders
    with ScalaPredefAndCollectionDecoders
    with ByteIterableDecoders
    with BigDecimalDecoders
    with TemporalDecoders
    with BaseDecoders {

  def apply[T](implicit decoder: Decoder[T]): Decoder[T] = decoder

  private class DelegatingDecoder[T, S](decoder: Decoder[T], val schemaFor: SchemaForV2[S], map: T => S)
      extends Decoder[S] {

    def decode(value: Any): S = map(decoder.decode(value))

    override def withSchema(schemaFor: SchemaForV2[S]): Decoder[S] = {
      // pass through decoder transformation.
      val decoderWithSchema = decoder.withSchema(schemaFor.forType)
      new DelegatingDecoder[T, S](decoderWithSchema, schemaFor.forType, map)
    }
  }

  implicit class DecoderFunctor[T](val decoder: Decoder[T]) extends AnyVal {
    def map[S](f: T => S): Decoder[S] = new DelegatingDecoder(decoder, decoder.schemaFor.forType, f)
  }
}
