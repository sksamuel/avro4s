package com.sksamuel.avro4s

import org.apache.avro.Schema

trait Codec[T] extends EncoderV2[T] with Decoder[T] with SchemaAware[Codec, T] {
  self =>

  def schema: Schema

  def encode(value: T): AnyRef

  def decode(value: Any): T

  override def withSchema(schemaFor: SchemaForV2[T]): Codec[T] = {
    val sf = schemaFor
    new Codec[T] {
      val schemaFor: SchemaForV2[T] = sf
      def encode(value: T): AnyRef = self.encode(value)
      def decode(value: Any): T = self.decode(value)
    }
  }
}

object Codec
    extends MagnoliaGeneratedCodecs
    with ShapelessCoproductCodecs
    with ScalaPredefAndCollectionCodecs
    with ByteIterableCodecs
    with BigDecimalCodecs
    with TemporalCodecs
    with BaseCodecs {

  def apply[T](implicit codec: Codec[T]): Codec[T] = codec

  private class DelegatingCodec[T, S](codec: Codec[T], val schemaFor: SchemaForV2[S], map: T => S, comap: S => T)
      extends Codec[S] {

    def encode(value: S): AnyRef = codec.encode(comap(value))

    def decode(value: Any): S = map(codec.decode(value))

    override def withSchema(schemaFor: SchemaForV2[S]): Codec[S] = {
      // pass through decoder transformation.
      val decoderWithSchema = codec.withSchema(schemaFor.forType)
      new DelegatingCodec[T, S](decoderWithSchema, schemaFor.forType, map, comap)
    }
  }

  implicit class CodecBifunctor[T](val codec: Codec[T]) extends AnyVal {
    def inmap[S](map: T => S, comap: S => T): Codec[S] = new DelegatingCodec(codec, codec.schemaFor.forType, map, comap)
  }
}
