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
    def schemaFor: SchemaForV2[(A, B)] = SchemaForV2.tuple2SchemaFor[A, B]
    def encode(value: (A, B)): AnyRef = encodeTuple2[A, B](value, schema)
    def decode(value: Any): (A, B) = decodeTuple2[A, B](value)
  }

  implicit def tuple3Codec[A: Codec, B: Codec, C: Codec] = new Codec[(A, B, C)] {
    import EncoderSchemaImplicits._
    def schemaFor: SchemaForV2[(A, B, C)] = SchemaForV2.tuple3SchemaFor[A, B, C]
    def encode(value: (A, B, C)): AnyRef = encodeTuple3[A, B, C](value, schema)
    def decode(value: Any): (A, B, C) = decodeTuple3[A, B, C](value)
  }

  implicit def tuple4Codec[A: Codec, B: Codec, C: Codec, D: Codec] = new Codec[(A, B, C, D)] {
    import EncoderSchemaImplicits._
    def schemaFor: SchemaForV2[(A, B, C, D)] = SchemaForV2.tuple4SchemaFor[A, B, C, D]
    def encode(value: (A, B, C, D)): AnyRef = encodeTuple4[A, B, C, D](value, schema)
    def decode(value: Any): (A, B, C, D) = decodeTuple4[A, B, C, D](value)
  }

  implicit def tuple5Codec[A: Codec, B: Codec, C: Codec, D: Codec, E: Codec] =
    new Codec[(A, B, C, D, E)] {
      import EncoderSchemaImplicits._
      def schemaFor: SchemaForV2[(A, B, C, D, E)] = SchemaForV2.tuple5SchemaFor[A, B, C, D, E]
      def encode(value: (A, B, C, D, E)): AnyRef = encodeTuple5[A, B, C, D, E](value, schema)
      def decode(value: Any): (A, B, C, D, E) = decodeTuple5[A, B, C, D, E](value)
    }
}

trait BaseEncoders {
  implicit val BooleanEncoder: Encoder[Boolean] = BaseTypes.BooleanCodec
  implicit val ByteBufferEncoder: Encoder[ByteBuffer] = BaseTypes.ByteBufferCodec
  implicit val ByteEncoder: Encoder[Byte] = BaseTypes.ByteCodec
  implicit val CharSequenceEncoder: Encoder[CharSequence] = BaseTypes.CharSequenceCodec
  implicit val DoubleEncoder: Encoder[Double] = BaseTypes.DoubleCodec
  implicit val FloatEncoder: Encoder[Float] = BaseTypes.FloatCodec
  implicit val IntEncoder: Encoder[Int] = BaseTypes.IntCodec
  implicit val LongEncoder: Encoder[Long] = BaseTypes.LongCodec
  implicit val ShortEncoder: Encoder[Short] = BaseTypes.ShortCodec
  implicit val StringEncoder: Encoder[String] = BaseTypes.StringCodec
  implicit val Utf8Encoder: Encoder[Utf8] = BaseTypes.Utf8Codec
  implicit val UUIDEncoder: Encoder[UUID] = BaseTypes.UUIDCodec
  implicit def javaEnumEncoder[E <: Enum[E]: ClassTag]: Encoder[E] = new JavaEnumCodec[E]
  implicit def scalaEnumEncoder[E <: Enumeration#Value: TypeTag]: Encoder[E] = new ScalaEnumCodec[E]

  implicit def tuple2Encoder[A: Encoder, B: Encoder] = new Encoder[(A, B)] {
    import EncoderSchemaImplicits._
    def schemaFor: SchemaForV2[(A, B)] = SchemaForV2.tuple2SchemaFor[A, B]
    def encode(value: (A, B)): AnyRef = encodeTuple2(value, schema)
  }

  implicit def tuple3Encoder[A: Encoder, B: Encoder, C: Encoder] = new Encoder[(A, B, C)] {
    import EncoderSchemaImplicits._
    def schemaFor: SchemaForV2[(A, B, C)] = SchemaForV2.tuple3SchemaFor[A, B, C]
    def encode(value: (A, B, C)): AnyRef = encodeTuple3(value, schema)
  }

  implicit def tuple4Encoder[A: Encoder, B: Encoder, C: Encoder, D: Encoder] = new Encoder[(A, B, C, D)] {
    import EncoderSchemaImplicits._
    def schemaFor: SchemaForV2[(A, B, C, D)] = SchemaForV2.tuple4SchemaFor[A, B, C, D]
    def encode(value: (A, B, C, D)): AnyRef = encodeTuple4(value, schema)
  }

  implicit def tuple5Encoder[A: Encoder, B: Encoder, C: Encoder, D: Encoder, E: Encoder] =
    new Encoder[(A, B, C, D, E)] {
      import EncoderSchemaImplicits._
      def schemaFor: SchemaForV2[(A, B, C, D, E)] = SchemaForV2.tuple5SchemaFor[A, B, C, D, E]
      def encode(value: (A, B, C, D, E)): AnyRef = encodeTuple5(value, schema)
    }
}

trait BaseDecoders {
  implicit val BooleanDecoder: Decoder[Boolean] = BaseTypes.BooleanCodec
  implicit val ByteBufferDecoder: Decoder[ByteBuffer] = BaseTypes.ByteBufferCodec
  implicit val ByteDecoder: Decoder[Byte] = BaseTypes.ByteCodec
  implicit val CharSequenceDecoder: Decoder[CharSequence] = BaseTypes.CharSequenceCodec
  implicit val DoubleDecoder: Decoder[Double] = BaseTypes.DoubleCodec
  implicit val FloatDecoder: Decoder[Float] = BaseTypes.FloatCodec
  implicit val IntDecoder: Decoder[Int] = BaseTypes.IntCodec
  implicit val LongDecoder: Decoder[Long] = BaseTypes.LongCodec
  implicit val ShortDecoder: Decoder[Short] = BaseTypes.ShortCodec
  implicit val StringDecoder: Decoder[String] = BaseTypes.StringCodec
  implicit val Utf8Decoder: Decoder[Utf8] = BaseTypes.Utf8Codec
  implicit val UUIDDecoder: Decoder[UUID] = BaseTypes.UUIDCodec
  implicit def javaEnumDecoder[E <: Enum[E]: ClassTag]: Decoder[E] = new JavaEnumCodec[E]
  implicit def scalaEnumEncoder[E <: Enumeration#Value: TypeTag]: Decoder[E] = new ScalaEnumCodec[E]

  implicit def tuple2Decoder[A: Decoder, B: Decoder] = new Decoder[(A, B)] {
    import DecoderSchemaImplicits._
    def schemaFor: SchemaForV2[(A, B)] = SchemaForV2.tuple2SchemaFor[A, B]
    def decode(value: Any): (A, B) = decodeTuple2[A, B](value)
  }

  implicit def tuple3Decoder[A: Decoder, B: Decoder, C: Decoder] = new Decoder[(A, B, C)] {
    import DecoderSchemaImplicits._
    def schemaFor: SchemaForV2[(A, B, C)] = SchemaForV2.tuple3SchemaFor[A, B, C]
    def decode(value: Any): (A, B, C) = decodeTuple3[A, B, C](value)
  }

  implicit def tuple4Decoder[A: Decoder, B: Decoder, C: Decoder, D: Decoder] = new Decoder[(A, B, C, D)] {
    import DecoderSchemaImplicits._
    def schemaFor: SchemaForV2[(A, B, C, D)] = SchemaForV2.tuple4SchemaFor[A, B, C, D]
    def decode(value: Any): (A, B, C, D) = decodeTuple4[A, B, C, D](value)
  }

  implicit def tuple5Decoder[A: Decoder, B: Decoder, C: Decoder, D: Decoder, E: Decoder] =
    new Decoder[(A, B, C, D, E)] {
      import DecoderSchemaImplicits._
      def schemaFor: SchemaForV2[(A, B, C, D, E)] = SchemaForV2.tuple5SchemaFor[A, B, C, D, E]
      def decode(value: Any): (A, B, C, D, E) = decodeTuple5[A, B, C, D, E](value)
    }
}

object BaseTypes {

  object ByteCodec extends Codec[Byte] {

    val schemaFor: SchemaForV2[Byte] = SchemaForV2.ByteSchema

    def encode(t: Byte): java.lang.Byte = java.lang.Byte.valueOf(t)

    override def decode(value: Any): Byte = value match {
      case b: Byte => b
      case _       => value.asInstanceOf[Int].byteValue
    }
  }

  object ShortCodec extends Codec[Short] {

    val schemaFor: SchemaForV2[Short] = SchemaForV2.ShortSchema

    def encode(t: Short): java.lang.Short = java.lang.Short.valueOf(t)

    override def decode(value: Any): Short = value match {
      case b: Byte  => b
      case s: Short => s
      case i: Int   => i.toShort
    }
  }

  object IntCodec extends Codec[Int] {

    val schemaFor: SchemaForV2[Int] = SchemaForV2.IntSchema

    def encode(value: Int): AnyRef = java.lang.Integer.valueOf(value)

    def decode(value: Any): Int = value match {
      case byte: Byte   => byte.toInt
      case short: Short => short.toInt
      case int: Int     => int
      case other        => sys.error(s"Cannot convert $other to type INT")
    }
  }

  object LongCodec extends Codec[Long] {

    val schemaFor: SchemaForV2[Long] = SchemaForV2.LongSchema

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

    val schemaFor: SchemaForV2[Double] = SchemaForV2.DoubleSchema

    def encode(value: Double): AnyRef = java.lang.Double.valueOf(value)

    def decode(value: Any): Double = value match {
      case d: Double           => d
      case d: java.lang.Double => d
    }
  }

  object FloatCodec extends Codec[Float] {

    val schemaFor: SchemaForV2[Float] = SchemaForV2.FloatSchema

    def encode(value: Float): AnyRef = java.lang.Float.valueOf(value)

    def decode(value: Any): Float = value match {
      case f: Float           => f
      case f: java.lang.Float => f
    }
  }

  object BooleanCodec extends Codec[Boolean] {

    val schemaFor: SchemaForV2[Boolean] = SchemaForV2.BooleanSchema

    def encode(value: Boolean): AnyRef = java.lang.Boolean.valueOf(value)

    def decode(value: Any): Boolean = value.asInstanceOf[Boolean]
  }

  object ByteBufferCodec extends Codec[ByteBuffer] {

    val schemaFor: SchemaForV2[ByteBuffer] = SchemaForV2.ByteBufferSchema

    def encode(value: ByteBuffer): AnyRef = value

    def decode(value: Any): ByteBuffer = value match {
      case b: ByteBuffer  => b
      case a: Array[Byte] => ByteBuffer.wrap(a)
      case _              => sys.error(s"Unable to decode value $value to ByteBuffer")
    }
  }

  object CharSequenceCodec extends Codec[CharSequence] {

    val schemaFor: SchemaForV2[CharSequence] = SchemaForV2.CharSequenceSchema

    def encode(value: CharSequence): AnyRef = value

    def decode(value: Any): CharSequence = value match {
      case cs: CharSequence => cs
      case _                => sys.error(s"Unable to decode value $value to CharSequence")
    }
  }

  val StringCodec: Codec[String] = new StringCodec(SchemaForV2.StringSchema)

  val Utf8Codec: Codec[Utf8] = new Codec[Utf8] {
    val schemaFor: SchemaForV2[Utf8] = SchemaForV2.Utf8Schema

    def encode(value: Utf8): AnyRef = value

    def decode(value: Any): Utf8 = value match {
      case u: Utf8        => u
      case b: Array[Byte] => new Utf8(b)
      case null           => sys.error("Cannot decode <null> as utf8")
      case _              => new Utf8(value.toString)
    }
  }

  private class StringCodec(val schemaFor: SchemaForV2[String]) extends Codec[String] {

    val encoder: String => AnyRef = schema.getType match {
      case Schema.Type.STRING => new Utf8(_)
      case Schema.Type.FIXED  => encodeFixed
      case Schema.Type.BYTES  => str => ByteBuffer.wrap(str.getBytes)
      case _                  => sys.error(s"Unsupported type for string schema: $schema")
    }

    def encodeFixed(value: String): AnyRef = {
      if (value.getBytes.length > schema.getFixedSize)
        sys.error(
          s"Cannot write string with ${value.getBytes.length} bytes to fixed type of size ${schema.getFixedSize}")
      GenericData.get.createFixed(null, ByteBuffer.allocate(schema.getFixedSize).put(value.getBytes).array, schema)
    }

    def encode(value: String): AnyRef = encoder(value)

    def decode(value: Any): String = value match {
      case u: Utf8             => u.toString
      case s: String           => s
      case chars: CharSequence => chars.toString
      case fixed: GenericFixed => new String(fixed.bytes())
      case a: Array[Byte]      => new String(a)
      case null                => sys.error("Cannot decode <null> as a string")
      case other               => sys.error(s"Cannot decode $other of type ${other.getClass} into a string")
    }

    override def withSchema(schemaFor: SchemaForV2[String]): Codec[String] = new StringCodec(schemaFor)
  }

  val UUIDCodec = StringCodec.inmap[UUID](UUID.fromString, _.toString)

  class JavaEnumCodec[E <: Enum[E]](implicit tag: ClassTag[E]) extends Codec[E] {
    val schemaFor: SchemaForV2[E] = SchemaForV2.javaEnumSchema

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

    val schemaFor: SchemaForV2[E] = SchemaForV2.scalaEnumSchema[E]

    def encode(value: E): AnyRef = new EnumSymbol(schema, value.toString)

    def decode(value: Any): E = enum.withName(value.toString).asInstanceOf[E]
  }

  def decodeTuple2[A, B](value: Any)(implicit
                                     decoderA: Decoder[A],
                                     decoderB: Decoder[B]) = {
    val record = value.asInstanceOf[GenericRecord]
    (
      decoderA.decode(record.get("_1")),
      decoderB.decode(record.get("_2"))
    )
  }

  def decodeTuple3[A, B, C](value: Any)(implicit
                                        decoderA: Decoder[A],
                                        decoderB: Decoder[B],
                                        decoderC: Decoder[C]) = {
    val record = value.asInstanceOf[GenericRecord]
    (
      decoderA.decode(record.get("_1")),
      decoderB.decode(record.get("_2")),
      decoderC.decode(record.get("_3"))
    )
  }

  def decodeTuple4[A, B, C, D](value: Any)(implicit
                                           decoderA: Decoder[A],
                                           decoderB: Decoder[B],
                                           decoderC: Decoder[C],
                                           decoderD: Decoder[D]) = {
    val record = value.asInstanceOf[GenericRecord]
    (
      decoderA.decode(record.get("_1")),
      decoderB.decode(record.get("_2")),
      decoderC.decode(record.get("_3")),
      decoderD.decode(record.get("_4"))
    )
  }

  def decodeTuple5[A, B, C, D, E](value: Any)(implicit
                                              decoderA: Decoder[A],
                                              decoderB: Decoder[B],
                                              decoderC: Decoder[C],
                                              decoderD: Decoder[D],
                                              decoderE: Decoder[E]) = {
    val record = value.asInstanceOf[GenericRecord]
    (
      decoderA.decode(record.get("_1")),
      decoderB.decode(record.get("_2")),
      decoderC.decode(record.get("_3")),
      decoderD.decode(record.get("_4")),
      decoderE.decode(record.get("_5"))
    )
  }

  def encodeTuple2[A, B](value: (A, B), schema: Schema)(implicit encoderA: Encoder[A], encoderB: Encoder[B]) = {
    ImmutableRecord(
      schema,
      Vector(encoderA.encode(value._1), encoderB.encode(value._2))
    )
  }

  def encodeTuple3[A, B, C](value: (A, B, C), schema: Schema)(implicit encoderA: Encoder[A],
                                                              encoderB: Encoder[B],
                                                              encoderC: Encoder[C]) = {
    ImmutableRecord(
      schema,
      Vector(encoderA.encode(value._1), encoderB.encode(value._2), encoderC.encode(value._3))
    )
  }

  def encodeTuple4[A, B, C, D](value: (A, B, C, D), schema: Schema)(implicit encoderA: Encoder[A],
                                                                    encoderB: Encoder[B],
                                                                    encoderC: Encoder[C],
                                                                    encoderD: Encoder[D]) = {
    ImmutableRecord(
      schema,
      Vector(encoderA.encode(value._1), encoderB.encode(value._2), encoderC.encode(value._3), encoderD.encode(value._4))
    )
  }

  def encodeTuple5[A, B, C, D, E](value: (A, B, C, D, E), schema: Schema)(implicit encoderA: Encoder[A],
                                                                          encoderB: Encoder[B],
                                                                          encoderC: Encoder[C],
                                                                          encoderD: Encoder[D],
                                                                          encoderE: Encoder[E]) = {
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
    implicit def schemaFromEncoder[T](implicit encoder: Encoder[T]): SchemaForV2[T] = SchemaForV2[T](encoder.schema)
  }

  private[avro4s] object DecoderSchemaImplicits {
    implicit def schemaFromDecoder[T](implicit decoder: Decoder[T]): SchemaForV2[T] = SchemaForV2[T](decoder.schema)
  }
}
