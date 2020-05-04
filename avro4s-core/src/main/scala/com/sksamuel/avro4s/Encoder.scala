package com.sksamuel.avro4s

import java.nio.ByteBuffer

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NoUpdate}
import org.apache.avro.generic.GenericData
import org.apache.avro.{Schema, SchemaBuilder}

import scala.language.experimental.macros

/**
  * An [[Encoder]] encodes a Scala value of type T into a compatible
  * Avro value based on the given schema.
  *
  * For example, given a string, and a schema of type Schema.Type.STRING
  * then the string would be encoded as an instance of Utf8, whereas
  * the same string and a Schema.Type.FIXED would be encoded as an
  * instance of GenericFixed.
  *
  * Another example is given a Scala enumeration value, and a schema of
  * type Schema.Type.ENUM, the value would be encoded as an instance
  * of GenericData.EnumSymbol.
  */
trait Encoder[T] extends Resolvable[Encoder, T] with SchemaAware[Encoder, T] with Serializable { self =>

  /**
    * Encodes the given value to a value supported by Avro's generic data model
    */
  def encode(value: T): AnyRef

  def withSchema(schemaFor: SchemaFor[T]): Encoder[T] = {
    val sf = schemaFor
    new Encoder[T] {
      val schemaFor: SchemaFor[T] = sf

      def encode(value: T): AnyRef = self.encode(value)
    }
  }

  def apply(env: DefinitionEnvironment[Encoder], update: SchemaUpdate): Encoder[T] = (self, update) match {
    case (resolvable: ResolvableEncoder[T], _) => resolvable.resolve(env, update)
    case (_, FullSchemaUpdate(sf))             => self.withSchema(sf.forType)
    case _                                     => self
  }
}

trait ResolvableEncoder[T] extends Encoder[T] {

  def resolve(env: DefinitionEnvironment[Encoder], update: SchemaUpdate): Encoder[T]

  lazy val adhocInstance = resolve(DefinitionEnvironment.empty, NoUpdate)

  def encode(value: T): AnyRef = adhocInstance.encode(value)

  def schemaFor: SchemaFor[T] = adhocInstance.schemaFor
}

object Encoder
    extends LowPrio
    with BigDecimalEncoders
    with TupleEncoders
    with TemporalEncoders
    with BaseEncoders {

  implicit val ByteArrayEncoder: Encoder[Array[Byte]] = new ByteArrayEncoderBase {
    val schemaFor = SchemaFor[Byte].map(SchemaBuilder.array.items(_))
    def encode(value: Array[Byte]): AnyRef = ByteBuffer.wrap(value)
  }

  private def iterableByteEncoder[C[X] <: Iterable[X]](build: Array[Byte] => C[Byte]): Encoder[C[Byte]] =
    new IterableByteEncoder[C](build)

  implicit val ByteListEncoder: Encoder[List[Byte]] = iterableByteEncoder(_.toList)
  implicit val ByteVectorEncoder: Encoder[Vector[Byte]] = iterableByteEncoder(_.toVector)
  implicit val ByteSeqEncoder: Encoder[Seq[Byte]] = iterableByteEncoder(_.toSeq)

  private sealed trait ByteArrayEncoderBase extends Encoder[Array[Byte]] {
    override def withSchema(schemaFor: SchemaFor[Array[Byte]]): Encoder[Array[Byte]] =
      schemaFor.schema.getType match {
        case Schema.Type.BYTES => ByteArrayEncoder
        case Schema.Type.FIXED => new FixedByteArrayEncoder(schemaFor)
        case _                 => sys.error(s"Byte array codec doesn't support schema type ${schemaFor.schema.getType}")
      }
  }

  private class FixedByteArrayEncoder(val schemaFor: SchemaFor[Array[Byte]]) extends ByteArrayEncoderBase {
    require(schema.getType == Schema.Type.FIXED)

    def encode(value: Array[Byte]): AnyRef = {
      val array = new Array[Byte](schema.getFixedSize)
      System.arraycopy(value, 0, array, 0, value.length)
      GenericData.get.createFixed(null, array, schema)
    }
  }

  private class IterableByteEncoder[C[X] <: Iterable[X]](build: Array[Byte] => C[Byte],
                                                         byteArrayEncoder: Encoder[Array[Byte]] = ByteArrayEncoder)
    extends Encoder[C[Byte]] {

    val schemaFor: SchemaFor[C[Byte]] = byteArrayEncoder.schemaFor.forType
    def encode(value: C[Byte]): AnyRef = byteArrayEncoder.encode(value.toArray)

    override def withSchema(schemaFor: SchemaFor[C[Byte]]): Encoder[C[Byte]] =
      new IterableByteEncoder(build, byteArrayEncoder.withSchema(schemaFor.map(identity)))
  }

  def apply[T](implicit encoder: Encoder[T]): Encoder[T] = encoder.apply()

  private class DelegatingEncoder[T, S](encoder: Encoder[T], val schemaFor: SchemaFor[S], comap: S => T)
      extends Encoder[S] {

    def encode(value: S): AnyRef = encoder.encode(comap(value))

    override def withSchema(schemaFor: SchemaFor[S]): Encoder[S] = {
      // pass through schema so that underlying encoder performs desired transformations.
      val modifiedEncoder = encoder.withSchema(schemaFor.forType)
      new DelegatingEncoder[T, S](modifiedEncoder, schemaFor.forType, comap)
    }
  }

  /**
    * Enables decorating/enhancing an encoder with a transformation function
    */
  implicit class EncoderOps[T](val encoder: Encoder[T]) extends AnyVal {
    def comap[S](f: S => T): Encoder[S] = new DelegatingEncoder(encoder, encoder.schemaFor.forType, f)
  }
}

trait LowPrio extends CollectionAndContainerEncoders with MagnoliaDerivedEncoders with ShapelessCoproductEncoders

object EncoderHelpers {
  def buildWithSchema[T](encoderRec: Encoder[T], schemaFor: SchemaFor[T]): Encoder[T] =
    encoderRec(DefinitionEnvironment.empty, FullSchemaUpdate(schemaFor))

  def mapFullUpdate(f: Schema => Schema, update: SchemaUpdate) = update match {
    case full: FullSchemaUpdate => FullSchemaUpdate(SchemaFor(f(full.schemaFor.schema), full.schemaFor.fieldMapper))
    case _                      => update
  }
}
