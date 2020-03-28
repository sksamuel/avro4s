package com.sksamuel.avro4s

/**
 * Converts back and forth between Avro Generic representation and its Scala / Java equivalent
 */
trait Codec[T] extends Encoder[T] with Decoder[T] with SchemaAware[Codec, T] {
  self =>

  override def withSchema(schemaFor: SchemaFor[T]): Codec[T] = {
    val sf = schemaFor
    new Codec[T] {
      val schemaFor: SchemaFor[T] = sf
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

  private class DelegatingCodec[T, S](codec: Codec[T], val schemaFor: SchemaFor[S], map: T => S, comap: S => T)
      extends Codec[S] {

    def encode(value: S): AnyRef = codec.encode(comap(value))

    def decode(value: Any): S = map(codec.decode(value))

    override def withSchema(schemaFor: SchemaFor[S]): Codec[S] = {
      // pass through schema so that underlying codec performs desired transformations.
      val modifiedCodec = codec.withSchema(schemaFor.forType)
      new DelegatingCodec[T, S](modifiedCodec, schemaFor.forType, map, comap)
    }
  }

  /**
   * Enables decorating/enhancing a codec with two back-and-forth transformation functions
   */
  implicit class CodecOps[T](val codec: Codec[T]) extends AnyVal {
    def inmap[S](map: T => S, comap: S => T): Codec[S] = new DelegatingCodec(codec, codec.schemaFor.forType, map, comap)
  }
}
