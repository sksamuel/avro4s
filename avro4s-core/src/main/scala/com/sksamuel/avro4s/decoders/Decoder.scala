package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.{DefaultFieldMapper, FieldMapper}
import com.sksamuel.avro4s.decoders.PrimitiveDecoders
import com.sksamuel.avro4s.encoders.{PrimitiveEncoders, StringEncoders}
import com.sksamuel.avro4s.schemas.Macros
import org.apache.avro.Schema

/**
 * An [[Decoder]] decodes a JVM value read from Avro into a scala type T.
 *
 * For example, a decoder could convert Avro UTF8 instances into plain Strings.
 *
 * Another example, a decoder for Option[String] would handle inputs of null
 * by emitting a None, and a non-null input by emitting the decoded value
 * wrapped in a Some.
 *
 * An encoder is invoked with an Avro schema, and a [[FieldMapper]] and returns
 * a reusable function that then decodes Avro values into Scala values of type T.
 *
 * It is possible to configure decoders entirely through annotations, which is fine if your 
 * system is self contained. But if your schemas are generated outside of avro4s, or even
 * in another language, you may need to use these "third-party" schemas to influence the
 * decoding process.
 *
 * Some decoders use the schema to determine the decoding function to return. For example, decimals
 * can be encoded using a specified precision and scale. When decoding, we need to know this precision
 * and scale in order to correctly decode the bytes. Therefore the Decoder[BigDecimal] typeclass instance 
 * uses the schema to correctly set these values.
 *
 * Other types may not need the schema at all. For instance, a Decoder[Int] can simply pass through
 * the integer provided by Avro.
 */
trait Decoder[T] {
  self =>

  /**
   * Decodes the given AvroValue to an instance of T.
   * Throws an exception if the value cannot be decoded.
   */
  def decode(schema: Schema, mapper: FieldMapper = DefaultFieldMapper): Any => T

  /**
   * Returns a Decoder[U] that first decodes to T by using this decoder, and then maps
   * the returned T value using the given T => U function.
   */
  def map[U](f: T => U): Decoder[U] = new Decoder[U] :
    override def decode(schema: Schema, mapper: FieldMapper): Any => U = { value => f(self.decode(schema, mapper)(value)) }
}

object Decoder extends PrimitiveDecoders with MacroDecoders {

  /**
   * Summons a macro derived decoder for the type T.
   */
  def apply[T](using decoder: Decoder[T]) = decoder

  /**
   * Returns a decoder for T by delegating to the given partial function.
   * This decoder will ignore any schema or mapper parameters.
   */
  def apply[T](pf: PartialFunction[Any, T]) = new Decoder[T] {
    override def decode(schema: Schema, mapper: FieldMapper): Any => T = { value => pf(value) }
  }
}