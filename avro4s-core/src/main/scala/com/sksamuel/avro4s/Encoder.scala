package com.sksamuel.avro4s

import com.sksamuel.avro4s.{DefaultFieldMapper, FieldMapper}
import com.sksamuel.avro4s.encoders.{BigDecimalEncoders, ByteIterableEncoders, CollectionEncoders, EitherEncoders, MagnoliaDerivedEncoder, OptionEncoders, PrimitiveEncoders, StringEncoders, TemporalEncoders, TupleEncoders}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

/**
  * An [[Encoder]] encodes a Scala value of type T into a JVM value suitable
  * for use with Avro.
  *
  * For example, an encoder could encode a String as an instance of [[Utf8]],
  * or it could encode it as an instance of [[GenericFixed]].
  *
  * Alternatively, given a Scala enum value, the enum could be encoded
  * as an instance of [[GenericData.EnumSymbol]] or as a String.
  *
  * An encoder is invoked with an Avro schema, and a [[FieldMapper]] and returns
  * a reusable function that then encodes values of type T into Avro types.
  *
  * It is possible to configure encoders entirely through annotations, which is fine if your
  * system is self contained. But if your schemas are generated outside of avro4s, or even
  * in another language, you may need to use these "third-party" schemas to influence the
  * encoding process.
  *
  * Some encoders use the schema to determine the encoding function to return. For example, strings
  * can be encoded as [[UTF8]]s, [[GenericFixed]]s, [[ByteBuffers]] or [[java.lang.String]]s.
  * Therefore the Encoder[String] typeclass instances uses the schema to select which of these
  * implementations to use.
  *
  * Other types may not require the schema at all. For example, the default Encoder[Int] always
  * returns a java.lang.Integer regardless of any schema input.
  *
  * The second parameter to an encoder is the field mapper. This is used to derive
  * the field names used when generating record or error types. By default, the field mapper will
  * use the field names as they are defined in the type itself (from the case class).
  * However for interop with other systems you may wish to customize this, for example, by
  * writing out field names in snake_case or adding a prefix.
  */
trait Encoder[T] extends Serializable {
  self =>

  def encode(schema: Schema): T => Any

  /**
    * Returns an [[Encoder[U]] by applying a function that maps a U
    * to an T, before encoding as an T using this encoder.
    */
  final def contramap[U](f: U => T): Encoder[U] = new Encoder[U] {
    override def encode(schema: Schema): U => Any = { u => self.encode(schema).apply(f(u)) }
  }
}

object Encoder
  extends PrimitiveEncoders
    with BigDecimalEncoders
    with ByteIterableEncoders
    with CollectionEncoders
    with EitherEncoders
    with OptionEncoders
    with StringEncoders
    with TemporalEncoders
    with TupleEncoders
    with MagnoliaDerivedEncoder {

  /**
    * Returns an [Encoder] that encodes using the supplied function.
    */
  def apply[T](f: T => Any) = new Encoder[T] {
    def encode(schema: Schema): T => Any = { t => f(t) }
  }

  /**
    * Returns an [Encoder] that encodes by simply returning the input value.
    */
  def identity[T]: Encoder[T] = Encoder[T](t => t)

  def apply[T](using encoder: Encoder[T]): Encoder[T] = encoder
}