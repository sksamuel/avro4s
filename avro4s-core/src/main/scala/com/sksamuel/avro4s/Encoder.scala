package com.sksamuel.avro4s

import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

import scala.deriving.Mirror
import scala.deriving._
import scala.compiletime.{erasedValue, summonInline}

/**
 * An [[Encoder]] encodes a Scala value of type T into an [[AvroValue]]
 * based on the given schema.
 *
 * For example, encoding a string with a schema of type Schema.Type.STRING
 * would result in an instance of Utf8, whereas the same string and a
 * schema of type Schema.Type.FIXED would be encoded as an instance of GenericFixed.
 *
 * Another example is given a Scala enum value, and a schema of
 * type Schema.Type.ENUM, the value would be encoded as an instance
 * of GenericData.EnumSymbol.
 */
trait Encoder[T] {
  def encode(value: T, schema: Schema): AvroValue
}

object Encoder extends BasicEncoders {

  inline given derived[T](using m: Mirror.Of[T]) as Encoder[T] = {

    inline m match {
      case s: Mirror.SumOf[T] => println("SumOf")
      case p: Mirror.ProductOf[T] => println("ProductOf")
    }

    new Encoder[T] {
      override def encode(value: T, schema: Schema): AvroValue =
        AvroValue.AvroString("foo")
    }
  }
}

trait BasicEncoders {

  given Encoder[Byte] :
    override def encode(value: Byte, schema: Schema): AvroValue = AvroValue.AvroByte(java.lang.Byte.valueOf(value))

  given Encoder[Short] :
    override def encode(value: Short, schema: Schema): AvroValue = AvroValue.AvroShort(java.lang.Short.valueOf(value))

  given Encoder[Int] :
    override def encode(value: Int, schema: Schema): AvroValue = AvroValue.AvroInt(java.lang.Integer.valueOf(value))

  given Encoder[Long] :
    override def encode(value: Long, schema: Schema): AvroValue = AvroValue.AvroLong(java.lang.Long.valueOf(value))

  given Encoder[Double] :
    override def encode(value: Double, schema: Schema): AvroValue = AvroValue.AvroDouble(java.lang.Double.valueOf(value))

  given Encoder[Float] :
    override def encode(value: Float, schema: Schema): AvroValue = AvroValue.AvroFloat(java.lang.Float.valueOf(value))

  given Encoder[Boolean] :
    override def encode(value: Boolean, schema: Schema): AvroValue = AvroValue.AvroBoolean(java.lang.Boolean.valueOf(value))

  given Encoder[ByteBuffer] :
    override def encode(value: ByteBuffer, schema: Schema): AvroValue = AvroValue.AvroByteBuffer(value)

  given Encoder[CharSequence] :
    override def encode(value: CharSequence, schema: Schema): AvroValue = AvroValue.AvroString(value.toString)

  given Encoder[String] :

    private def encodeFixed(value: String, schema: Schema): GenericData.Fixed = {
      if (value.getBytes.length > schema.getFixedSize)
        throw new Avro4sEncodingException(s"Cannot write string with ${value.getBytes.length} bytes to fixed type of size ${schema.getFixedSize}")
      GenericData.get.createFixed(null, ByteBuffer.allocate(schema.getFixedSize).put(value.getBytes).array, schema).asInstanceOf[GenericData.Fixed]
    }

    override def encode(value: String, schema: Schema): AvroValue = schema.getType match {
      case Schema.Type.STRING => AvroValue.AvroUtf8(new Utf8(value))
      case Schema.Type.FIXED => AvroValue.Fixed(encodeFixed(value, schema))
      case Schema.Type.BYTES => AvroValue.AvroByteArray(value.getBytes)
      case _ => throw new Avro4sConfigurationException(s"Unsupported type for string schema: $schema")
    }

  given Encoder[Utf8] :
    override def encode(value: Utf8, schema: Schema): AvroValue = AvroValue.AvroUtf8(value)
}