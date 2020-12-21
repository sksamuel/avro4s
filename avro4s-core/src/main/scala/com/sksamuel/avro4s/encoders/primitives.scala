package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{Encoder, EncoderFor}

import java.nio.ByteBuffer

trait PrimitiveEncoders {
  given EncoderFor[Byte] = EncoderFor(a => java.lang.Byte.valueOf(a))
  given EncoderFor[Short] = EncoderFor(a => java.lang.Short.valueOf(a))
  given EncoderFor[Int] = EncoderFor(a => java.lang.Integer.valueOf(a))
  given EncoderFor[Long] = EncoderFor(a => java.lang.Long.valueOf(a))
  given EncoderFor[Double] = EncoderFor(a => java.lang.Double.valueOf(a))
  given EncoderFor[Float] = EncoderFor(a => java.lang.Float.valueOf(a))
  given EncoderFor[Boolean] = EncoderFor(a => java.lang.Boolean.valueOf(a))
  given EncoderFor[ByteBuffer] = EncoderFor.identity
}