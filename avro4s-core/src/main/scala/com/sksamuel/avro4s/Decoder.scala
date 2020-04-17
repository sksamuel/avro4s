package com.sksamuel.avro4s

import java.nio.ByteBuffer

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NoUpdate}
import org.apache.avro.generic.GenericFixed
import org.apache.avro.{Schema, SchemaBuilder}

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
    extends TupleDecoders
    with CollectionAndContainerDecoders
    with ShapelessCoproductDecoders
    with MagnoliaDerivedDecoders
    with BigDecimalDecoders
    with TemporalDecoders
    with BaseDecoders {

  implicit val ByteArrayDecoder: Decoder[Array[Byte]] = new ByteArrayDecoderBase {
    val schemaFor = SchemaFor(SchemaBuilder.builder().bytesType())
  }

  implicit val ByteListDecoder: Decoder[List[Byte]] = iterableByteDecoder(_.toList)
  implicit val ByteVectorDecoder: Decoder[Vector[Byte]] = iterableByteDecoder(_.toVector)
  implicit val ByteSeqDecoder: Decoder[Seq[Byte]] = iterableByteDecoder(_.toSeq)

  private def iterableByteDecoder[C[X] <: Iterable[X]](build: Array[Byte] => C[Byte]): Decoder[C[Byte]] =
    new IterableByteDecoder[C](build)

  private sealed trait ByteArrayDecoderBase extends Decoder[Array[Byte]] {

    def decode(value: Any): Array[Byte] = value match {
      case buffer: ByteBuffer  => buffer.array
      case array: Array[Byte]  => array
      case fixed: GenericFixed => fixed.bytes
      case _                   => sys.error(s"Byte array codec cannot decode '$value'")
    }

    override def withSchema(schemaFor: SchemaFor[Array[Byte]]): Decoder[Array[Byte]] =
      schemaFor.schema.getType match {
        case Schema.Type.BYTES => ByteArrayDecoder
        case Schema.Type.FIXED => new FixedByteArrayDecoder(schemaFor)
        case _                 => sys.error(s"Byte array codec doesn't support schema type ${schemaFor.schema.getType}")
      }
  }

  private class FixedByteArrayDecoder(val schemaFor: SchemaFor[Array[Byte]]) extends ByteArrayDecoderBase {
    require(schema.getType == Schema.Type.FIXED)
  }

  private class IterableByteDecoder[C[X] <: Iterable[X]](build: Array[Byte] => C[Byte],
                                                         byteArrayDecoder: Decoder[Array[Byte]] = ByteArrayDecoder)
      extends Decoder[C[Byte]] {

    val schemaFor: SchemaFor[C[Byte]] = byteArrayDecoder.schemaFor.forType

    def decode(value: Any): C[Byte] = build(byteArrayDecoder.decode(value))

    override def withSchema(schemaFor: SchemaFor[C[Byte]]): Decoder[C[Byte]] =
      new IterableByteDecoder(build, byteArrayDecoder.withSchema(schemaFor.map(identity)))
  }

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
