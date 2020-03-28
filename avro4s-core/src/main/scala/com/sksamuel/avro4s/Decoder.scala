package com.sksamuel.avro4s

/**
 * Transforms an Avro Generic data type to its Scala / Java representation
 *
 * @tparam T data type this decoder reads
 */
trait Decoder[T] extends SchemaAware[Decoder, T] with Serializable { self =>

  /**
   * Decodes the given Avro Generic value to a value of type T
   */
  def decode(value: Any): T

  def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = {
    val sf = schemaFor
    new Decoder[T] {
      def schemaFor: SchemaFor[T] = sf
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

  private class DelegatingDecoder[T, S](decoder: Decoder[T], val schemaFor: SchemaFor[S], map: T => S)
      extends Decoder[S] {

    def decode(value: Any): S = map(decoder.decode(value))

    override def withSchema(schemaFor: SchemaFor[S]): Decoder[S] = {
      // pass through schema so that underlying decoder performs desired transformations.
      val modifiedDecoder = decoder.withSchema(schemaFor.forType)
      new DelegatingDecoder[T, S](modifiedDecoder, schemaFor.forType, map)
    }
  }

  /**
   * Enables decorating/enhancing a decoder with a transformation function
   */
  implicit class DecoderOps[T](val decoder: Decoder[T]) extends AnyVal {
    def map[S](f: T => S): Decoder[S] = new DelegatingDecoder(decoder, decoder.schemaFor.forType, f)
  }
}
