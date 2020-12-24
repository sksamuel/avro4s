package com.sksamuel.avro4s

import com.sksamuel.avro4s.encoders.{MacroEncoder, PrimitiveEncoders, StringEncoders}
import com.sksamuel.avro4s.schemas.MacroSchemaFor
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

import scala.deriving.Mirror

/**
 * An [[Encoder]] encodes a Scala value of type T into a JVM value suitable
 * for encoding with Avro.
 *
 * Encoders use a schema which controls how they encode a particular type.
 *
 * For example, an encoder could encode a String as an instance of Utf8,
 * or it could encode as an instance of GenericFixed.
 *
 * Another example is given a Scala enum value, the value could be encoded
 * as an instance of GenericData.EnumSymbol.
 *
 */
trait Encoder[T] {
  self =>

  def encode(schema: Schema): T => Any

  /**
   * Returns an [[Encoder[U]] by applying a function that maps a U
   * to an T, before encoding as an T using this encoder.
   */
  def contramap[U](f: U => T): Encoder[U] = new Encoder[U] {
    override def encode(schema: Schema): U => Any = { u => self.encode(schema)(f(u)) }
  }
}

object Encoder extends PrimitiveEncoders with StringEncoders {
  
  def apply[T](using encoder: Encoder[T]) = encoder

  /**
   * Creates an [[Encoder]] for T by using a macro derived implementation.
   */
  inline given derive[T](using m: Mirror.Of[T]): Encoder[T] = MacroEncoder.derive[T]

  /**
   * Returns an [Encoder] that encodes using the supplied function without reference to a schema.
   */
  def apply[T](f: T => Any) = new Encoder[T] {
    override def encode(schema: Schema): T => Any = f
  }

  def identity[T] = new Encoder[T] {
    override def encode(schema: Schema): T => Any = { t => t }
  }
}