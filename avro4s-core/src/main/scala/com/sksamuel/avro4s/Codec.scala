package com.sksamuel.avro4s

import org.apache.avro.Schema

trait Codec[T] extends EncoderV2[T] with DecoderV2[T] with SchemaAware[Codec, T] {
  self =>

  def schema: Schema

  def encode(value: T): AnyRef

  def decode(value: Any): T

  override def withSchema(schemaFor: SchemaForV2[T]): Codec[T] = new Codec[T] {
    val schema: Schema = schemaFor.schema

    def encode(value: T): AnyRef = self.encode(value)

    def decode(value: Any): T = self.decode(value)
  }
}

object Codec
    extends MagnoliaGeneratedCodecs
    with ShapelessCoproductCodecs
    with ScalaPredefAndCollectionCodecs
    with BigDecimalCodecs
    with TemporalCodecs
    with BaseCodecs {

  def apply[T](implicit codec: Codec[T]): Codec[T] = codec

  private class DelegatingCodec[T, S](codec: Codec[T], val schema: Schema, map: T => S, comap: S => T)
      extends Codec[S] {

    def encode(value: S): AnyRef = codec.encode(comap(value))

    def decode(value: Any): S = map(codec.decode(value))

    override def withSchema(schemaFor: SchemaForV2[S]): Codec[S] = {
      // pass through decoder transformation.
      val decoderWithSchema = codec.withSchema(schemaFor.map(identity))
      new DelegatingCodec[T, S](decoderWithSchema, schemaFor.schema, map, comap)
    }
  }

  implicit class CodecBifunctor[T](val codec: Codec[T]) extends AnyVal {
    def inmap[S](map: T => S, comap: S => T): Codec[S] = new DelegatingCodec(codec, codec.schema, map, comap)
  }
}
