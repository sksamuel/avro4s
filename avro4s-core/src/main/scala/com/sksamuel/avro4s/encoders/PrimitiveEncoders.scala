package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{Encoder}

import java.nio.ByteBuffer

trait PrimitiveEncoders {

  given Encoder[Long] = Encoder(a => java.lang.Long.valueOf(a))
  given Encoder[Int] = Encoder(a => java.lang.Integer.valueOf(a))
  given Encoder[Byte] = Encoder(a => java.lang.Byte.valueOf(a))
  given Encoder[Short] = Encoder(a => java.lang.Short.valueOf(a))
  given Encoder[Double] = Encoder(a => java.lang.Double.valueOf(a))
  given Encoder[Float] = Encoder(a => java.lang.Float.valueOf(a))
  given Encoder[Boolean] = Encoder(a => java.lang.Boolean.valueOf(a))
}