package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.util.UUID

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericFixed}
import org.apache.avro.util.Utf8

trait BaseCodecs {
  implicit val BooleanCodec: Codec[Boolean] = BaseTypes.BooleanCodec
  implicit val ByteBufferCodec: Codec[ByteBuffer] = BaseTypes.ByteBufferCodec
  implicit val ByteCodec: Codec[Byte] = BaseTypes.ByteCodec
  implicit val CharSequenceCodec: Codec[CharSequence] = BaseTypes.CharSequenceCodec
  implicit val DoubleCodec: Codec[Double] = BaseTypes.DoubleCodec
  implicit val FloatCodec: Codec[Float] = BaseTypes.FloatCodec
  implicit val IntCodec: Codec[Int] = BaseTypes.IntCodec
  implicit val LongCodec: Codec[Long] = BaseTypes.LongCodec
  implicit val ShortCodec: Codec[Short] = BaseTypes.ShortCodec
  implicit val StringCodec: Codec[String] = BaseTypes.StringCodec
  implicit val UUIDCodec: Codec[UUID] = BaseTypes.UUIDCodec
}

trait BaseEncoders {
  implicit val BooleanEncoder: EncoderV2[Boolean] = BaseTypes.BooleanCodec
  implicit val ByteBufferEncoder: EncoderV2[ByteBuffer] = BaseTypes.ByteBufferCodec
  implicit val ByteEncoder: EncoderV2[Byte] = BaseTypes.ByteCodec
  implicit val CharSequenceEncoder: EncoderV2[CharSequence] = BaseTypes.CharSequenceCodec
  implicit val DoubleEncoder: EncoderV2[Double] = BaseTypes.DoubleCodec
  implicit val FloatEncoder: EncoderV2[Float] = BaseTypes.FloatCodec
  implicit val IntEncoder: EncoderV2[Int] = BaseTypes.IntCodec
  implicit val LongEncoder: EncoderV2[Long] = BaseTypes.LongCodec
  implicit val ShortEncoder: EncoderV2[Short] = BaseTypes.ShortCodec
  implicit val StringEncoder: EncoderV2[String] = BaseTypes.StringCodec
  implicit val UUIDEncoder: EncoderV2[UUID] = BaseTypes.UUIDCodec
}

trait BaseDecoders {
  implicit val BooleanDecoder: DecoderV2[Boolean] = BaseTypes.BooleanCodec
  implicit val ByteBufferDecoder: DecoderV2[ByteBuffer] = BaseTypes.ByteBufferCodec
  implicit val ByteDecoder: DecoderV2[Byte] = BaseTypes.ByteCodec
  implicit val CharSequenceDecoder: DecoderV2[CharSequence] = BaseTypes.CharSequenceCodec
  implicit val DoubleDecoder: DecoderV2[Double] = BaseTypes.DoubleCodec
  implicit val FloatDecoder: DecoderV2[Float] = BaseTypes.FloatCodec
  implicit val IntDecoder: DecoderV2[Int] = BaseTypes.IntCodec
  implicit val LongDecoder: DecoderV2[Long] = BaseTypes.LongCodec
  implicit val ShortDecoder: DecoderV2[Short] = BaseTypes.ShortCodec
  implicit val StringDecoder: DecoderV2[String] = BaseTypes.StringCodec
  implicit val UUIDDecoder: DecoderV2[UUID] = BaseTypes.UUIDCodec
}

object BaseTypes {

  object ByteCodec extends Codec[Byte] {

    val schema: Schema = SchemaForV2.ByteSchema.schema

    def encode(t: Byte): java.lang.Byte = java.lang.Byte.valueOf(t)

    override def decode(value: Any): Byte = value match {
      case b: Byte => b
      case _       => value.asInstanceOf[Int].byteValue
    }
  }

  object ShortCodec extends Codec[Short] {

    val schema: Schema = SchemaForV2.ShortSchema.schema

    def encode(t: Short): java.lang.Short = java.lang.Short.valueOf(t)

    override def decode(value: Any): Short = value match {
      case b: Byte  => b
      case s: Short => s
      case i: Int   => i.toShort
    }
  }

  object IntCodec extends Codec[Int] {

    val schema: Schema = SchemaForV2.IntSchema.schema

    def encode(value: Int): AnyRef = java.lang.Integer.valueOf(value)

    def decode(value: Any): Int = value match {
      case byte: Byte   => byte.toInt
      case short: Short => short.toInt
      case int: Int     => int
      case other        => sys.error(s"Cannot convert $other to type INT")
    }
  }

  object LongCodec extends Codec[Long] {

    val schema: Schema = SchemaForV2.LongSchema.schema

    def encode(value: Long): AnyRef = java.lang.Long.valueOf(value)

    def decode(value: Any): Long = value match {
      case byte: Byte   => byte.toLong
      case short: Short => short.toLong
      case int: Int     => int.toLong
      case long: Long   => long
      case other        => sys.error(s"Cannot convert $other to type LONG")
    }
  }

  object DoubleCodec extends Codec[Double] {

    val schema: Schema = SchemaForV2.DoubleSchema.schema

    def encode(value: Double): AnyRef = java.lang.Double.valueOf(value)

    def decode(value: Any): Double = value match {
      case d: Double           => d
      case d: java.lang.Double => d
    }
  }

  object FloatCodec extends Codec[Float] {

    val schema: Schema = SchemaForV2.FloatSchema.schema

    def encode(value: Float): AnyRef = java.lang.Float.valueOf(value)

    def decode(value: Any): Float = value match {
      case f: Float           => f
      case f: java.lang.Float => f
    }
  }

  object BooleanCodec extends Codec[Boolean] {

    val schema: Schema = SchemaForV2.BooleanSchema.schema

    def encode(value: Boolean): AnyRef = java.lang.Boolean.valueOf(value)

    def decode(value: Any): Boolean = value.asInstanceOf[Boolean]
  }

  object ByteBufferCodec extends Codec[ByteBuffer] {

    val schema: Schema = SchemaForV2.ByteBufferSchema.schema

    def encode(value: ByteBuffer): AnyRef = value

    def decode(value: Any): ByteBuffer = value match {
      case b: ByteBuffer => b
      case _             => sys.error(s"Unable to decode value $value to ByteBuffer")
    }
  }

  object CharSequenceCodec extends Codec[CharSequence] {

    val schema: Schema = SchemaForV2.CharSequenceSchema.schema

    def encode(value: CharSequence): AnyRef = value

    def decode(value: Any): CharSequence = value match {
      case cs: CharSequence => cs
      case _                => sys.error(s"Unable to decode value $value to CharSequence")
    }
  }

  private sealed trait StringCodecBase extends Codec[String] {
    override def withSchema(schemaFor: SchemaForV2[String]): Codec[String] = {
      val schema = schemaFor.schema
      schema.getType match {
        case Schema.Type.STRING => new StringCodec(schema)
        case Schema.Type.FIXED  => new FixedStringCodec(schema)
        case Schema.Type.BYTES  => new ByteStringCodec(schema)
        case _                  => sys.error(s"Unsupported type for string schema: $schema")
      }
    }
  }

  val StringCodec: Codec[String] = new StringCodec(SchemaForV2.StringSchema.schema)

  private class StringCodec(val schema: Schema) extends StringCodecBase {
    require(schema.getType == Schema.Type.STRING)

    def encode(value: String): AnyRef = new Utf8(value)

    def decode(value: Any): String = value match {
      case u: Utf8             => u.toString
      case s: String           => s
      case chars: CharSequence => chars.toString
      case null                => sys.error("Cannot decode <null> as a string")
      case other               => sys.error(s"Cannot decode $other of type ${other.getClass} into a string")
    }
  }

  private class FixedStringCodec(val schema: Schema) extends StringCodecBase {
    require(schema.getType == Schema.Type.FIXED, s"Fixed string schema must be of type FIXED, got ${schema.getType}")

    def encode(value: String): AnyRef = {
      if (value.getBytes.length > schema.getFixedSize)
        sys.error(
          s"Cannot write string with ${value.getBytes.length} bytes to fixed type of size ${schema.getFixedSize}")
      GenericData.get.createFixed(null, ByteBuffer.allocate(schema.getFixedSize).put(value.getBytes).array, schema)
    }

    def decode(value: Any): String = value match {
      case fixed: GenericFixed => new String(fixed.bytes())
      case null                => sys.error("Cannot decode <null> as a string")
      case other               => sys.error(s"Cannot decode $other of type ${other.getClass} into a string")
    }
  }

  private class ByteStringCodec(val schema: Schema) extends Codec[String] {
    require(schema.getType == Schema.Type.BYTES)

    def encode(value: String): AnyRef = ByteBuffer.wrap(value.getBytes)

    def decode(value: Any): String = value match {
      case bytebuf: ByteBuffer => new String(bytebuf.array)
      case a: Array[Byte]      => new String(a)
      case null                => sys.error("Cannot decode <null> as a string")
      case other               => sys.error(s"Cannot decode $other of type ${other.getClass} into a string")
    }
  }

  val UUIDCodec = StringCodec.inmap[UUID](UUID.fromString, _.toString)
}
