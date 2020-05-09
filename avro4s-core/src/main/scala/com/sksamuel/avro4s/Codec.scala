package com.sksamuel.avro4s


trait Codec[T] extends Encoder[T] with Decoder[T] with SchemaAware[Codec, T] { self =>

  override def withSchema(schemaFor: SchemaFor[T]): Codec[T] = {
    val sf = schemaFor
    new Codec[T] {
      def schemaFor: SchemaFor[T] = sf
      def encode(value: T): AnyRef = self.encode(value)
      def decode(value: Any): T = self.decode(value)
    }
  }
}

object Codec {

  /**
   * Enables decorating/enhancing a codec with two transformation functions
   */
  implicit class CodecOps[T](val codec: Codec[T]) extends AnyVal {
    def inmap[S](map: T => S, comap: S => T): Decoder[S] = new DelegatingCodec(codec, codec.schemaFor.forType, map, comap)
  }

  private class DelegatingCodec[T, S](codec: Codec[T], val schemaFor: SchemaFor[S], map: T => S, comap: S => T)
    extends Codec[S] {

    def encode(value: S): AnyRef = codec.encode(comap(value))

    def decode(value: Any): S = map(codec.decode(value))

    override def withSchema(schemaFor: SchemaFor[S]): Codec[S] = {
      // pass through schema so that underlying decoder performs desired transformations.
      val modifiedCodec = codec.withSchema(schemaFor.forType)
      new DelegatingCodec[T, S](modifiedCodec, schemaFor.forType, map, comap)
    }
  }
}