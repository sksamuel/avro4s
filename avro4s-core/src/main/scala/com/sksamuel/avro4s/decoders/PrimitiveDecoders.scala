package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.{Avro4sDecodingException, BasicDecoder, Decoder}
import org.apache.avro.Schema

trait PrimitiveDecoders {

  given Decoder[Byte] = new BasicDecoder[Byte] {
    override def decode(value: Any): Byte = value match {
      case b: Byte => b
      case _ => value.asInstanceOf[Int].byteValue
    }
  }

  given Decoder[Short] = new BasicDecoder[Short] {
    override def decode(value: Any): Short = value match {
      case b: Byte => b
      case s: Short => s
      case i: Int => i.toShort
    }
  }

  given Decoder[Int] = new BasicDecoder[Int] {
    override def decode(value: Any): Int = value match {
      case byte: Byte => byte.toInt
      case short: Short => short.toInt
      case int: Int => int
      case other => throw new Avro4sDecodingException(s"Cannot convert $other to type INT", value)
    }
  }

  given Decoder[Long] = new BasicDecoder[Long] {
    override def decode(value: Any): Long = value match {
      case byte: Byte => byte.toLong
      case short: Short => short.toLong
      case int: Int => int.toLong
      case long: Long => long
      case other => throw new Avro4sDecodingException(s"Cannot convert $other to type LONG", value)
    }
  }

  given Decoder[Double] = new BasicDecoder[Double] {
    override def decode(value: Any): Double = value match {
      case d: Double => d
      case d: java.lang.Double => d
    }
  }

  given Decoder[Float] = new BasicDecoder[Float] {
    override def decode(value: Any): Float = value match {
      case f: Float => f
      case f: java.lang.Float => f
    }
  }

  given Decoder[Boolean] = new BasicDecoder[Boolean] {
    override def decode(value: Any): Boolean = value.asInstanceOf[Boolean]
  }
}
