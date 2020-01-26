package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.util.UUID

import com.sksamuel.avro4s.BaseTypes.{JavaEnumCodec, ScalaEnumCodec}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.{GenericData, GenericFixed, GenericRecord}
import org.apache.avro.util.Utf8
import BaseTypes._

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

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
  implicit val Utf8Codec: Codec[Utf8] = BaseTypes.Utf8Codec
  implicit val UUIDCodec: Codec[UUID] = BaseTypes.UUIDCodec
  implicit def javaEnumCodec[E <: Enum[E]: ClassTag]: Codec[E] = new JavaEnumCodec[E]
  implicit def scalaEnumEncoder[E <: Enumeration#Value: TypeTag]: Codec[E] = new ScalaEnumCodec[E]
  
  implicit def tuple2Codec[A: Codec, B: Codec] = new Codec[(A, B)] {
    import EncoderSchemaImplicits._
    def schema: Schema = SchemaForV2.tuple2SchemaFor[A, B].schema
    def encode(value: (A, B)): AnyRef = encodeTuple2[A, B](value, schema)
    def decode(value: Any): (A, B) = decodeTuple2[A, B](value)
  }

  implicit def tuple3Codec[A: Codec, B: Codec, C: Codec] = new Codec[(A, B, C)] {
    import EncoderSchemaImplicits._
    def schema: Schema = SchemaForV2.tuple3SchemaFor[A, B, C].schema
    def encode(value: (A, B, C)): AnyRef = encodeTuple3[A, B, C](value, schema)
    def decode(value: Any): (A, B, C) = decodeTuple3[A, B, C](value)
  }

  implicit def tuple4Codec[A: Codec, B: Codec, C: Codec, D: Codec] = new Codec[(A, B, C, D)] {
    import EncoderSchemaImplicits._
    def schema: Schema = SchemaForV2.tuple4SchemaFor[A, B, C, D].schema
    def encode(value: (A, B, C, D)): AnyRef = encodeTuple4[A, B, C, D](value, schema)
    def decode(value: Any): (A, B, C, D) = decodeTuple4[A, B, C, D](value)
  }

  implicit def tuple5Codec[A: Codec, B: Codec, C: Codec, D: Codec, E: Codec] =
    new Codec[(A, B, C, D, E)] {
      import EncoderSchemaImplicits._
      def schema: Schema = SchemaForV2.tuple5SchemaFor[A, B, C, D, E].schema
      def encode(value: (A, B, C, D, E)): AnyRef = encodeTuple5[A, B, C, D, E](value, schema)
      def decode(value: Any): (A, B, C, D, E) = decodeTuple5[A, B, C, D, E](value)
    }
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
  implicit val Utf8Encoder: EncoderV2[Utf8] = BaseTypes.Utf8Codec
  implicit val UUIDEncoder: EncoderV2[UUID] = BaseTypes.UUIDCodec
  implicit def javaEnumEncoder[E <: Enum[E]: ClassTag]: EncoderV2[E] = new JavaEnumCodec[E]
  implicit def scalaEnumEncoder[E <: Enumeration#Value: TypeTag]: EncoderV2[E] = new ScalaEnumCodec[E]

  implicit def tuple2Encoder[A: EncoderV2, B: EncoderV2] = new EncoderV2[(A, B)] {
    import EncoderSchemaImplicits._
    def schema: Schema = SchemaForV2.tuple2SchemaFor[A, B].schema
    def encode(value: (A, B)): AnyRef = encodeTuple2(value, schema)
  }

  implicit def tuple3Encoder[A: EncoderV2, B: EncoderV2, C: EncoderV2] = new EncoderV2[(A, B, C)] {
    import EncoderSchemaImplicits._
    def schema: Schema = SchemaForV2.tuple3SchemaFor[A, B, C].schema
    def encode(value: (A, B, C)): AnyRef = encodeTuple3(value, schema)
  }

  implicit def tuple4Encoder[A: EncoderV2, B: EncoderV2, C: EncoderV2, D: EncoderV2] = new EncoderV2[(A, B, C, D)] {
    import EncoderSchemaImplicits._
    def schema: Schema = SchemaForV2.tuple4SchemaFor[A, B, C, D].schema
    def encode(value: (A, B, C, D)): AnyRef = encodeTuple4(value, schema)
  }

  implicit def tuple5Encoder[A: EncoderV2, B: EncoderV2, C: EncoderV2, D: EncoderV2, E: EncoderV2] =
    new EncoderV2[(A, B, C, D, E)] {
      import EncoderSchemaImplicits._
      def schema: Schema = SchemaForV2.tuple5SchemaFor[A, B, C, D, E].schema
      def encode(value: (A, B, C, D, E)): AnyRef = encodeTuple5(value, schema)
    }
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
  implicit val Utf8Decoder: DecoderV2[Utf8] = BaseTypes.Utf8Codec
  implicit val UUIDDecoder: DecoderV2[UUID] = BaseTypes.UUIDCodec
  implicit def javaEnumDecoder[E <: Enum[E]: ClassTag]: DecoderV2[E] = new JavaEnumCodec[E]
  implicit def scalaEnumEncoder[E <: Enumeration#Value: TypeTag]: DecoderV2[E] = new ScalaEnumCodec[E]

  implicit def tuple2Decoder[A: DecoderV2, B: DecoderV2] = new DecoderV2[(A, B)] {
    import DecoderSchemaImplicits._
    def schema: Schema = SchemaForV2.tuple2SchemaFor[A, B].schema
    def decode(value: Any): (A, B) = decodeTuple2[A, B](value)
  }

  implicit def tuple3Decoder[A: DecoderV2, B: DecoderV2, C: DecoderV2] = new DecoderV2[(A, B, C)] {
    import DecoderSchemaImplicits._
    def schema: Schema = SchemaForV2.tuple3SchemaFor[A, B, C].schema
    def decode(value: Any): (A, B, C) = decodeTuple3[A, B, C](value)
  }

  implicit def tuple4Decoder[A: DecoderV2, B: DecoderV2, C: DecoderV2, D: DecoderV2] = new DecoderV2[(A, B, C, D)] {
    import DecoderSchemaImplicits._
    def schema: Schema = SchemaForV2.tuple4SchemaFor[A, B, C, D].schema
    def decode(value: Any): (A, B, C, D) = decodeTuple4[A, B, C, D](value)
  }

  implicit def tuple5Decoder[A: DecoderV2, B: DecoderV2, C: DecoderV2, D: DecoderV2, E: DecoderV2] = new DecoderV2[(A, B, C, D, E)] {
    import DecoderSchemaImplicits._
    def schema: Schema = SchemaForV2.tuple5SchemaFor[A, B, C, D, E].schema
    def decode(value: Any): (A, B, C, D, E) = decodeTuple5[A, B, C, D, E](value)
  }
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
      case b: ByteBuffer  => b
      case a: Array[Byte] => ByteBuffer.wrap(a)
      case _              => sys.error(s"Unable to decode value $value to ByteBuffer")
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

  val Utf8Codec: Codec[Utf8] = new Codec[Utf8] {
    val schema: Schema = SchemaForV2.Utf8Schema.schema

    def encode(value: Utf8): AnyRef = value

    def decode(value: Any): Utf8 = value match {
      case u: Utf8        => u
      case b: Array[Byte] => new Utf8(b)
      case null           => sys.error("Cannot decode <null> as utf8")
      case _              => new Utf8(value.toString)
    }
  }

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
      case a: Array[Byte]      => new String(a)
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

  class JavaEnumCodec[E <: Enum[E]](implicit tag: ClassTag[E]) extends Codec[E] {
    val schema: Schema = SchemaForV2.javaEnumSchema.schema

    def encode(value: E): AnyRef = new EnumSymbol(schema, value.name)

    def decode(value: Any): E = Enum.valueOf(tag.runtimeClass.asInstanceOf[Class[E]], value.toString)
  }

  class ScalaEnumCodec[E <: Enumeration#Value](implicit tag: TypeTag[E]) extends Codec[E] {
    val mirror: Mirror = runtimeMirror(getClass.getClassLoader)

    val enum = tag.tpe match {
      case TypeRef(enumType, _, _) =>
        val moduleSymbol = enumType.termSymbol.asModule
        mirror.reflectModule(moduleSymbol).instance.asInstanceOf[Enumeration]
    }

    val schema: Schema = SchemaForV2.scalaEnumSchema[E].schema

    def encode(value: E): AnyRef = new EnumSymbol(schema, value.toString)

    def decode(value: Any): E = enum.withName(value.toString).asInstanceOf[E]
  }

  def decodeTuple2[A, B](value: Any)(implicit
                                     decoderA: DecoderV2[A],
                                     decoderB: DecoderV2[B]) = {
    val record = value.asInstanceOf[GenericRecord]
    (
      decoderA.decode(record.get("_1")),
      decoderB.decode(record.get("_2"))
    )
  }

  def decodeTuple3[A, B, C](value: Any)(implicit
                                        decoderA: DecoderV2[A],
                                        decoderB: DecoderV2[B],
                                        decoderC: DecoderV2[C]) = {
    val record = value.asInstanceOf[GenericRecord]
    (
      decoderA.decode(record.get("_1")),
      decoderB.decode(record.get("_2")),
      decoderC.decode(record.get("_3"))
    )
  }

  def decodeTuple4[A, B, C, D](value: Any)(implicit
                                           decoderA: DecoderV2[A],
                                           decoderB: DecoderV2[B],
                                           decoderC: DecoderV2[C],
                                           decoderD: DecoderV2[D]) = {
    val record = value.asInstanceOf[GenericRecord]
    (
      decoderA.decode(record.get("_1")),
      decoderB.decode(record.get("_2")),
      decoderC.decode(record.get("_3")),
      decoderD.decode(record.get("_4"))
    )
  }

  def decodeTuple5[A, B, C, D, E](value: Any)(implicit
                                              decoderA: DecoderV2[A],
                                              decoderB: DecoderV2[B],
                                              decoderC: DecoderV2[C],
                                              decoderD: DecoderV2[D],
                                              decoderE: DecoderV2[E]) = {
    val record = value.asInstanceOf[GenericRecord]
    (
      decoderA.decode(record.get("_1")),
      decoderB.decode(record.get("_2")),
      decoderC.decode(record.get("_3")),
      decoderD.decode(record.get("_4")),
      decoderE.decode(record.get("_5"))
    )
  }

  def encodeTuple2[A, B](value: (A, B), schema: Schema)(implicit encoderA: EncoderV2[A], encoderB: EncoderV2[B]) = {
    ImmutableRecord(
      schema,
      Vector(encoderA.encode(value._1), encoderB.encode(value._2))
    )
  }

  def encodeTuple3[A, B, C](value: (A, B, C), schema: Schema)(implicit encoderA: EncoderV2[A],
                                                              encoderB: EncoderV2[B],
                                                              encoderC: EncoderV2[C]) = {
    ImmutableRecord(
      schema,
      Vector(encoderA.encode(value._1), encoderB.encode(value._2), encoderC.encode(value._3))
    )
  }

  def encodeTuple4[A, B, C, D](value: (A, B, C, D), schema: Schema)(implicit encoderA: EncoderV2[A],
                                                                    encoderB: EncoderV2[B],
                                                                    encoderC: EncoderV2[C],
                                                                    encoderD: EncoderV2[D]) = {
    ImmutableRecord(
      schema,
      Vector(encoderA.encode(value._1), encoderB.encode(value._2), encoderC.encode(value._3), encoderD.encode(value._4))
    )
  }

  def encodeTuple5[A, B, C, D, E](value: (A, B, C, D, E), schema: Schema)(implicit encoderA: EncoderV2[A],
                                                                          encoderB: EncoderV2[B],
                                                                          encoderC: EncoderV2[C],
                                                                          encoderD: EncoderV2[D],
                                                                          encoderE: EncoderV2[E]) = {
    ImmutableRecord(
      schema,
      Vector(encoderA.encode(value._1),
             encoderB.encode(value._2),
             encoderC.encode(value._3),
             encoderD.encode(value._4),
             encoderE.encode(value._5))
    )
  }

  private[avro4s] object EncoderSchemaImplicits {
    implicit def schemaFromEncoder[T](implicit encoder: EncoderV2[T]): SchemaForV2[T] = SchemaForV2[T](encoder.schema)
  }

  private[avro4s] object DecoderSchemaImplicits {
    implicit def schemaFromDecoder[T](implicit decoder: DecoderV2[T]): SchemaForV2[T] = SchemaForV2[T](decoder.schema)
  }
}
