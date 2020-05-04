package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NoUpdate}
import org.apache.avro.Schema

import scala.language.experimental.macros

/**
  * A [[Decoder]] is used to convert an Avro value, such as a GenericRecord,
  * SpecificRecord, GenericFixed, EnumSymbol, or a basic JVM type, into a
  * target Scala type.
  *
  * For example, a Decoder[String] would convert an input of type Utf8 -
  * which is one of the ways Avro can encode strings - into a plain Java String.
  *
  * Another example, a decoder for Option[String] would handle inputs of null
  * by emitting a None, and a non-null input by emitting the decoded value
  * wrapped in a Some.
  *
  * A final example is converting a GenericData.Array or a Java collection type
  * into a Scala collection type.
  */
trait Decoder[T] extends Resolvable[Decoder, T] with SchemaAware[Decoder, T] with Serializable { self =>

  /**
    * Decodes the given value to an instance of T if possible.
    * Otherwise throw an error.
    */
  def decode(value: Any): T

  def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = {
    val sf = schemaFor
    new Decoder[T] {
      def schemaFor: SchemaFor[T] = sf
      def decode(value: Any): T = self.decode(value)
    }
  }

  def apply(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[T] = (self, update) match {
    case (unresolved: ResolvableDecoder[T], _) => unresolved.resolve(env, update)
    case (_, FullSchemaUpdate(sf))             => self.withSchema(sf.forType)
    case _                                     => self
  }
}

trait ResolvableDecoder[T] extends Decoder[T] {

  def resolve(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[T]

  lazy val adhocInstance = resolve(DefinitionEnvironment.empty, NoUpdate)

  def decode(value: Any): T = adhocInstance.decode(value)

  def schemaFor: SchemaFor[T] = adhocInstance.schemaFor
}

object Decoder
    extends MagnoliaDerivedDecoders
    with ShapelessCoproductDecoders
    with CollectionAndContainerDecoders
    with TupleDecoders
    with ByteIterableDecoders
    with BigDecimalDecoders
    with TemporalDecoders
    with BaseDecoders {

  def apply[T](implicit decoder: Decoder[T]): Decoder[T] = decoder.apply()

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

object DecoderHelpers {
  def buildWithSchema[T](decoder: Decoder[T], schemaFor: SchemaFor[T]): Decoder[T] =
    decoder(DefinitionEnvironment.empty, FullSchemaUpdate(schemaFor))

  def mapFullUpdate(f: Schema => Schema, update: SchemaUpdate) = update match {
    case full: FullSchemaUpdate => FullSchemaUpdate(SchemaFor(f(full.schemaFor.schema), full.schemaFor.fieldMapper))
    case _                      => update
  }
}
