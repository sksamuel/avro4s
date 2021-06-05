//package com.sksamuel.avro4s
//
//import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NoUpdate}
//import org.apache.avro.Schema
//
//import scala.language.experimental.macros
//
///**
//  * A [[Decoder]] is used to convert an Avro value, such as a GenericRecord,
//  * SpecificRecord, GenericFixed, EnumSymbol, or a basic JVM type, into a
//  * target Scala type.
//  *
//  * For example, a Decoder[String] would convert an input of type Utf8 -
//  * which is one of the ways Avro can encode strings - into a plain Java String.
//  *
//  * Another example, a decoder for Option[String] would handle inputs of null
//  * by emitting a None, and a non-null input by emitting the decoded value
//  * wrapped in a Some.
//  *
//  * A final example is converting a GenericData.Array or a Java collection type
//  * into a Scala collection type.
//  */
//trait Decoder[T] extends SchemaAware[Decoder, T] with Serializable { self =>
//
//  /**
//    * Decodes the given value to an instance of T if possible.
//    * Otherwise throw an error.
//    */
//  def decode(value: Any): T
//
//  /**
//    * Creates a variant of this Decoder using the given schema (e.g. to use a fixed schema for byte arrays instead of
//    * the default bytes schema)
//    *
//    * @param schemaFor the schema to use
//    */
//  def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = {
//    val sf = schemaFor
//    new Decoder[T] {
//      def schemaFor: SchemaFor[T] = sf
//      def decode(value: Any): T = self.decode(value)
//    }
//  }
//
//  /**
//    * produces a Decoder that is guaranteed to be resolved and ready to be used.
//    *
//    * This is necessary for properly setting up Decoder instances for recursive types.
//    */
//  def resolveDecoder(): Decoder[T] = resolveDecoder(DefinitionEnvironment.empty, NoUpdate)
//
//  /**
//    * For advanced use only to properly setup Decoder instances for recursive types.
//    *
//    * Resolves the Decoder with the provided environment, and (potentially) pushes down overrides from annotations on
//    * sealed traits to case classes, or from annotations on parameters to types.
//    *
//    * @param env definition environment containing already defined record encoders
//    * @param update schema changes to apply
//    */
//  def resolveDecoder(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[T] = (self, update) match {
//    case (resolvable: ResolvableDecoder[T], _) => resolvable.decoder(env, update)
//    case (_, FullSchemaUpdate(sf))             => self.withSchema(sf.forType)
//    case _                                     => self
//  }
//
//}
//
///**
//  * A Decoder that needs to be resolved before it is usable. Resolution is needed to properly setup Decoder instances
//  * for recursive types.
//  *
//  * If this instance is used without resolution, it falls back to use an adhoc-resolved instance and delegates all
//  * operations to it. This involves a performance penalty of lazy val access that can be avoided by
//  * calling [[Encoder.resolveEncoder]] and using that.
//  *
//  * For examples on how to define custom ResolvableDecoder instances, see the Readme and RecursiveDecoderTest.
//  *
//  * @tparam T type this encoder is for (primitive type, case class, sealed trait, or enum e.g.).
//  */
//trait ResolvableDecoder[T] extends Decoder[T] {
//
//  /**
//    * Creates a Decoder instance (and applies schema changes given) or returns an already existing value from the
//    * given definition environment.
//    *
//    * @param env definition environment to use
//    * @param update schema update to apply
//    * @return either an already existing value from env or a new created instance.
//    */
//  def decoder(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[T]
//
//  lazy val adhocInstance = decoder(DefinitionEnvironment.empty, NoUpdate)
//
//  def decode(value: Any): T = adhocInstance.decode(value)
//
//  def schemaFor: SchemaFor[T] = adhocInstance.schemaFor
//
//  override def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = adhocInstance.withSchema(schemaFor)
//}
//
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
