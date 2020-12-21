package com.sksamuel.avro4s

//
//import java.nio.ByteBuffer
//import java.util.UUID
//

import com.sksamuel.avro4s.encoders.{PrimitiveEncoders, StringEncoders}
import org.apache.avro.Schema
//import org.apache.avro.generic.{GenericData, GenericRecord}
//import org.apache.avro.specific.SpecificRecord
//import org.apache.avro.util.Utf8
//
//import scala.deriving.Mirror
//import scala.deriving._
//import scala.compiletime.{erasedValue, summonInline}

/**
 * An [[Encoder]] encodes a Scala value of type T into a JVM value suitable
 * for encoding with Avro.
 *
 * For example, an encoder could encode a String as an instance of Utf8,
 * or it could encode as an instance of GenericFixed.
 *
 * Another example is given a Scala enum value, the value could be encoded
 * as an instance of GenericData.EnumSymbol.
 *
 * Encoders can be created from a schema which controls how they encode
 * a particular type. See [[Encoder.create(schema)]].
 */
trait Encoder[T] { self =>
  
  def encode(value: T): Any

  /**
   * Returns an [[Encoder[U]] by applying a function that maps a U
   * to an T, before encoding as an T using this encoder.
   */
  def contramap[U](f: U => T): Encoder[U] = new Encoder[U] {
    override def encode(value: U) = self.encode(f(value))
  }
}

object Encoder {

  /**
   * Creates an [[Encoder]] from the given schema, by using an implicit [[EncoderFor]].
   * The encoderFor instance will be derived automatically using macros if it is not explicitly
   * provided.
   */
  def apply[T](schema: Schema)(using encoderFor: EncoderFor[T]): Encoder[T] = encoderFor.encoder(schema)

  def apply[T](f: T => Any) = new Encoder[T] {
    override def encode(value: T): Any = f(value)
  }
}

/**
 * A typeclass that generates [[Encoder]] instances for a particular type from schemas.
 */
trait EncoderFor[T] { self =>
  
  /**
   * Returns an [[Encoder[T]] when provided a schema
   */
  def encoder(schema: Schema): Encoder[T]

  /**
   * Returns an [[EncoderFor[U]] by applying a function that maps a U
   * to an T, before encoding as an T using this encoder.
   */
  def contramap[U](f: U => T): EncoderFor[U] = new EncoderFor[U] {
    override def encoder(schema: Schema): Encoder[U] = self.encoder(schema).contramap(f)
  }
}

object EncoderFor extends StringEncoders with PrimitiveEncoders {

  def apply[T](f: T => Any) = new EncoderFor[T] {
    override def encoder(schema: Schema): Encoder[T] = new Encoder[T] {
      override def encode(value: T): Any = f(value)
    }
  }

  def identity[T] = new EncoderFor[T] {
    override def encoder(schema: Schema): Encoder[T] = new Encoder[T] {
      override def encode(value: T): Any = value
    }
  }
}


//
///**
// * An implementation of org.apache.avro.generic.GenericContainer that is both a
// * GenericRecord and a SpecificRecord.
// */
//trait Record extends GenericRecord with SpecificRecord
//
//case class ImmutableRecord(schema: Schema, values: IndexedSeq[Any]) extends Record {
//
//  require(schema.getType == Schema.Type.RECORD, "Cannot create an ImmutableRecord with a schema that is not a RECORD")
//  require(schema.getFields.size == values.size,
//    s"Schema field size (${schema.getFields.size}) and value Seq size (${values.size}) must match")
//
//  override def put(key: String, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")
//  override def put(i: Int, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")
//
//  override def get(key: String): Any = {
//    val field = schema.getField(key)
//    if (field == null) sys.error(s"Field $key does not exist in this record (schema=$schema, values=$values)")
//    values(field.pos)
//  }
//
//  override def get(i: Int): Any = values(i)
//  override def getSchema: Schema = schema
//}