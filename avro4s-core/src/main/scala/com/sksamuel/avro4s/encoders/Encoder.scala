package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{DefaultFieldMapper, FieldMapper}
import com.sksamuel.avro4s.encoders.{MacroEncoder, PrimitiveEncoders, StringEncoders}
import com.sksamuel.avro4s.schemas.MacroSchemaFor
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
 */
trait Encoder[T] {
  self =>

  def encode(schema: Schema, mapper: FieldMapper = DefaultFieldMapper): T => Any

  /**
   * Creates a variant of this Encoder using the given schema.
   *
   * For example, to use a fixed schema for byte arrays instead of
   * the default bytes schema.
   *
   * By default, the same encoder instance is returned. Encoders that wish
   * to use a schema to influence their behavior should override this method.
   */

  /**
   * Returns an [[Encoder[U]] by applying a function that maps a U
   * to an T, before encoding as an T using this encoder.
   */
  def contramap[U](f: U => T): Encoder[U] = new Encoder[U] {
    override def encode(schema: Schema, mapper: FieldMapper): U => Any = { u => self.encode(schema, mapper)(f(u)) }
  }
}

object Encoder extends PrimitiveEncoders 
  with StringEncoders
  with OptionEncoders
  with LowPriorityEncoders {

  def apply[T](using encoder: Encoder[T]) = encoder

  /**
   * Returns an [Encoder] that encodes using the supplied function.
   */
  def apply[T](f: T => Any) = new Encoder[T] {
    override def encode(schema: Schema, mapper: FieldMapper): T => Any = { t => f(t) }
  }

  /**
   * Returns an [Encoder] that encodes by simply returning the input value.
   */
  def identity[T] = Encoder[T](t => t)
}