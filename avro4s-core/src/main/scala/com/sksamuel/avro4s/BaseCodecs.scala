package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.util.UUID

import org.apache.avro.generic.{GenericData, GenericFixed}
import org.apache.avro.{Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.reflect.ClassTag

trait BaseCodecs {

  implicit val IntCodec: Codec[Int] = BaseTypes.IntCodec

  implicit val LongCodec: Codec[Long] = BaseTypes.LongCodec

  implicit val DoubleCodec: Codec[Double] = BaseTypes.DoubleCodec

  implicit val FloatCodec: Codec[Float] = BaseTypes.FloatCodec

  implicit val BooleanCodec: Codec[Boolean] = BaseTypes.BooleanCodec

  implicit val ByteBufferCodec: Codec[ByteBuffer] = BaseTypes.ByteBufferCodec

  implicit val CharSequenceCodec: Codec[CharSequence] = BaseTypes.CharSequenceCodec

  implicit def optionCodec[T](implicit codec: Codec[T]): Codec[Option[T]] = new Codec[Option[T]] {
    val schema: Schema = SchemaForV2.optionSchema(SchemaForV2[T](codec.schema)).schema

    def encode(value: Option[T]): AnyRef = value.map(codec.encode).orNull

    def decode(value: Any): Option[T] = if (value == null) None else Option(codec.decode(value))
  }

  sealed trait ByteArrayCodecBase extends Codec[Array[Byte]] with FieldSpecificCodec[Array[Byte]] {

    def decode(value: Any): Array[Byte] = value match {
      case buffer: ByteBuffer  => buffer.array
      case array: Array[Byte]  => array
      case fixed: GenericFixed => fixed.bytes
      case _                   => sys.error(s"Byte array codec cannot decode '$value'")
    }

    def forFieldWith(schema: Schema, annotations: Seq[Any]): ByteArrayCodecBase = withSchema(SchemaForV2(schema))

    override def withSchema(schemaFor: SchemaForV2[Array[Byte]]): ByteArrayCodecBase =
      schemaFor.schema.getType match {
        case Schema.Type.ARRAY => _byteArrayCodec
        case Schema.Type.FIXED => new FixedByteArrayCodec(schema)
        case _                 => sys.error(s"Byte array codec doesn't support schema type ${schema.getType}")
      }
  }

  private val _byteArrayCodec = new ByteArrayCodecBase {

    val schema: Schema = SchemaBuilder.builder.bytesType

    def encode(value: Array[Byte]): AnyRef = ByteBuffer.wrap(value)
  }

  implicit val ByteArrayCodec: Codec[Array[Byte]] = _byteArrayCodec

  class FixedByteArrayCodec(val schema: Schema) extends ByteArrayCodecBase {
    require(schema.getType == Schema.Type.FIXED)

    def encode(value: Array[Byte]): AnyRef = {
      val array = new Array[Byte](schema.getFixedSize)
      System.arraycopy(value, 0, array, 0, value.length)
      GenericData.get.createFixed(null, array, schema)
    }
  }

  private class IterableByteCodec[C[X] <: Iterable[X]](cbf: CanBuildFrom[Nothing, Byte, C[Byte]],
                                                       byteArrayCodec: ByteArrayCodecBase = _byteArrayCodec)
      extends Codec[C[Byte]]
      with FieldSpecificCodec[C[Byte]] {

    val schema = byteArrayCodec.schema

    def encode(value: C[Byte]): AnyRef = byteArrayCodec.encode(value.toArray)

    def decode(value: Any): C[Byte] = byteArrayCodec.decode(value).to[C](cbf)

    def forFieldWith(schema: Schema, annotations: Seq[Any]): Codec[C[Byte]] =
      new IterableByteCodec(cbf, byteArrayCodec.forFieldWith(schema, annotations))

    override def withSchema(schemaFor: SchemaForV2[C[Byte]]): Codec[C[Byte]] =
      forFieldWith(schemaFor.schema, Seq.empty)
  }

  implicit def iterableByteCodec[C[X] <: Iterable[X]](
      implicit cbf: CanBuildFrom[Nothing, Byte, C[Byte]]): Codec[C[Byte]] = new IterableByteCodec[C](cbf)

  implicit def arrayCodec[T: ClassTag](implicit codec: Codec[T]): Codec[Array[T]] = new Codec[Array[T]] {
    import scala.collection.JavaConverters._

    val schema: Schema = SchemaBuilder.array().items(codec.schema)

    def encode(value: Array[T]): AnyRef = value.map(codec.encode).toList.asJava

    def decode(value: Any): Array[T] = value match {
      case array: Array[_]               => array.map(codec.decode)
      case list: java.util.Collection[_] => list.asScala.map(codec.decode).toArray
      case list: Iterable[_]             => list.map(codec.decode).toArray
      case other                         => sys.error("Unsupported array " + other)
    }
  }

  implicit def iterableCodec[T, C[X] <: Iterable[X]](implicit codec: Codec[T],
                                                     cbf: CanBuildFrom[Nothing, T, C[T]]): Codec[C[T]] =
    new Codec[C[T]] {
      val schema: Schema = SchemaBuilder.array().items(codec.schema)

      def encode(value: C[T]): AnyRef = value.map(codec.encode).toList.asJava

      def decode(value: Any): C[T] = value match {
        case array: Array[_]               => array.map(codec.decode)(collection.breakOut)
        case list: java.util.Collection[_] => list.asScala.map(codec.decode)(collection.breakOut)
        case list: Iterable[_]             => list.map(codec.decode)(collection.breakOut)
        case other                         => sys.error("Unsupported array " + other)
      }
    }
}

trait BaseEncoders {}

trait BaseDecoders {}

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

  val UUIDCodec = Codec.stringCodec.inmap[UUID](UUID.fromString, _.toString)
}
