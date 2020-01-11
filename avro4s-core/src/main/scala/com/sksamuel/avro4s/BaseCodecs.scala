package com.sksamuel.avro4s

import java.time.Instant

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import scala.collection.JavaConverters._

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
