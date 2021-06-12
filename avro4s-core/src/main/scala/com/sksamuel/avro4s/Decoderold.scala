//object Decoder
//    extends MagnoliaDerivedDecoders
//    with ShapelessCoproductDecoders
//    with CollectionAndContainerDecoders
//    with TupleDecoders
//    with ByteIterableDecoders
//    with BigDecimalDecoders
//    with TemporalDecoders
//    with BaseDecoders {
//
//  def apply[T](implicit decoder: Decoder[T]): Decoder[T] = decoder
//
//  private class DelegatingDecoder[T, S](decoder: Decoder[T], val schemaFor: SchemaFor[S], map: T => S)
//      extends Decoder[S] {
//
//    def decode(value: Any): S = map(decoder.decode(value))
//
//    override def withSchema(schemaFor: SchemaFor[S]): Decoder[S] = {
//      // pass through schema so that underlying decoder performs desired transformations.
//      val modifiedDecoder = decoder.withSchema(schemaFor.forType)
//      new DelegatingDecoder[T, S](modifiedDecoder, schemaFor.forType, map)
//    }
//  }
//
//  /**
//    * Enables decorating/enhancing a decoder with a transformation function
//    */
//  implicit class DecoderOps[T](val decoder: Decoder[T]) extends AnyVal {
//    def map[S](f: T => S): Decoder[S] = new DelegatingDecoder(decoder, decoder.schemaFor.forType, f)
//  }
//}
//
//object DecoderHelpers {
//  def buildWithSchema[T](decoder: Decoder[T], schemaFor: SchemaFor[T]): Decoder[T] =
//    decoder.resolveDecoder(DefinitionEnvironment.empty, FullSchemaUpdate(schemaFor))
//
//  def mapFullUpdate(f: Schema => Schema, update: SchemaUpdate) = update match {
//    case full: FullSchemaUpdate => FullSchemaUpdate(SchemaFor(f(full.schemaFor.schema), full.schemaFor.fieldMapper))
//    case _                      => update
//  }
//}
