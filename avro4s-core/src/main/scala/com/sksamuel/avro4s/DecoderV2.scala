package com.sksamuel.avro4s

import org.apache.avro.Schema

trait DecoderV2[T] extends SchemaAware[DecoderV2, T] with Serializable { self =>

  def schema: Schema

  def decode(value: Any): T

  def withSchema(schemaFor: SchemaForV2[T]): DecoderV2[T] = new DecoderV2[T] {
    val schema: Schema = schemaFor.schema

    def decode(value: Any): T = self.decode(value)
  }
}

object DecoderV2
    extends MagnoliaGeneratedDecoders
    with ShapelessCoproductDecoders
    with ScalaPredefAndCollectionDecoders
    with ByteIterableDecoders
    with BigDecimalDecoders
    with TemporalDecoders
    with BaseDecoders {

  def apply[T](implicit decoder: DecoderV2[T]): DecoderV2[T] = decoder

  private class DelegatingDecoder[T, S](decoder: DecoderV2[T], val schema: Schema, map: T => S) extends DecoderV2[S] {

    def decode(value: Any): S = map(decoder.decode(value))

    override def withSchema(schemaFor: SchemaForV2[S]): DecoderV2[S] = {
      // pass through decoder transformation.
      val decoderWithSchema = decoder.withSchema(schemaFor.map(identity))
      new DelegatingDecoder[T, S](decoderWithSchema, schemaFor.schema, map)
    }
  }

  implicit class DecoderFunctor[T](val decoder: DecoderV2[T]) extends AnyVal {
    def map[S](f: T => S): DecoderV2[S] = new DelegatingDecoder(decoder, decoder.schema, f)
  }
}
