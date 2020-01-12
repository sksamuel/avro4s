package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.time.Instant

import org.apache.avro.generic.{GenericData, GenericFixed}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

trait BaseCodecs extends StringCodecs {

  implicit object IntCodec extends Codec[Int] {

    val schema: Schema = SchemaBuilder.builder.intType

    def encode(value: Int): AnyRef = java.lang.Integer.valueOf(value)

    def decode(value: Any): Int = value match {
      case byte: Byte   => byte.toInt
      case short: Short => short.toInt
      case int: Int     => int
      case other        => sys.error(s"Cannot convert $other to type INT")
    }
  }

  implicit object BooleanCodec extends Codec[Boolean] {

    val schema: Schema = SchemaBuilder.builder.booleanType

    def encode(value: Boolean): AnyRef = java.lang.Boolean.valueOf(value)

    def decode(value: Any): Boolean = value.asInstanceOf[Boolean]
  }

  implicit object InstantCodec extends Codec[Instant] {
    def schema: Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)

    def encode(value: Instant): AnyRef = new java.lang.Long(value.toEpochMilli)

    def decode(value: Any): Instant = value match {
      case long: Long => Instant.ofEpochMilli(long)
      case other      => sys.error(s"Cannot convert $other to type Instant")
    }
  }

  implicit def optionCodec[T](implicit codec: Codec[T]): Codec[Option[T]] = new Codec[Option[T]] {
    val schema: Schema = SchemaBuilder.nullable().`type`(codec.schema)

    def encode(value: Option[T]): AnyRef = value.map(codec.encode).orNull

    def decode(value: Any): Option[T] = if (value == null) None else Option(codec.decode(value))
  }

  sealed trait ByteArrayCodecBase extends Codec[Array[Byte]] with FieldSpecificSchemaTypeCodec[Array[Byte]] {

    def decode(value: Any): Array[Byte] = value match {
      case buffer: ByteBuffer  => buffer.array
      case array: Array[Byte]  => array
      case fixed: GenericFixed => fixed.bytes
    }

    def withFieldSchema(schema: Schema): ByteArrayCodecBase = schema.getType match {
      case Schema.Type.ARRAY => byteArrayCodec
      case Schema.Type.FIXED => new FixedByteArrayCodec(schema)
    }
  }

  implicit object byteArrayCodec extends ByteArrayCodecBase {

    val schema: Schema = SchemaBuilder.builder.bytesType

    def encode(value: Array[Byte]): AnyRef = ByteBuffer.wrap(value)
  }

  class FixedByteArrayCodec(val schema: Schema) extends ByteArrayCodecBase {
    require(schema.getType == Schema.Type.FIXED)

    def encode(value: Array[Byte]): AnyRef = {
      val bb = ByteBuffer.allocate(schema.getFixedSize).put(value)
      GenericData.get.createFixed(null, bb.array(), schema)
    }
  }

  class ByteSeqCodec[T[_]](map: Array[Byte] => T[Byte],
                           comap: T[Byte] => Array[Byte],
                           codec: ByteArrayCodecBase = byteArrayCodec)
      extends Codec[T[Byte]]
      with FieldSpecificSchemaTypeCodec[T[Byte]] {

    val schema = codec.schema

    def encode(value: T[Byte]): AnyRef = codec.encode(comap(value))

    def decode(value: Any): T[Byte] = map(codec.decode(value))

    def withFieldSchema(schema: Schema): Codec[T[Byte]] = new ByteSeqCodec(map, comap, codec.withFieldSchema(schema))
  }

  implicit val byteSeqCodec = new ByteSeqCodec[Seq](_.toSeq, _.toArray)
  implicit val byteListCodec = new ByteSeqCodec[List](_.toList, _.toArray)
  implicit val byteVectorCodec = new ByteSeqCodec[Vector](_.toVector, _.toArray)

  implicit def arrayCodec[T: ClassTag](implicit codec: Codec[T]): Codec[Array[T]] = new Codec[Array[T]] {
    import scala.collection.JavaConverters._

    val schema: Schema = SchemaBuilder.array().items(codec.schema)

    def encode(value: Array[T]): AnyRef = value.map(codec.encode).toList.asJava

    def decode(value: Any): Array[T] = value match {
      case array: Array[_]               => array.map(codec.decode)
      case list: java.util.Collection[_] => list.asScala.map(codec.decode).toArray
      case list: List[_]                 => list.map(codec.decode).toArray
      case other                         => sys.error("Unsupported array " + other)
    }
  }

  implicit def setCodec[S](implicit codec: Codec[S]): Codec[Set[S]] = {
    new Codec[Set[S]] {
      val schema: Schema = Schema.createArray(codec.schema)

      def encode(value: Set[S]): AnyRef = value.map(codec.encode).toList.asJava

      def decode(value: Any): Set[S] = {
        value match {
          case array: Array[_]               => array.map(codec.decode).toSet
          case list: java.util.Collection[_] => list.asScala.map(codec.decode).toSet
          case list: List[_]                 => list.map(codec.decode).toSet
          case other                         => sys.error("Unsupported array " + other)
        }
      }
    }
  }

}
