package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.Encoder
import org.apache.avro.Schema

import java.nio.ByteBuffer

trait PrimitiveEncoders {
  given LongEncoder: Encoder[Long] = Encoder(a => java.lang.Long.valueOf(a))
  given Encoder[Int] = IntEncoder
  given ShortEncoder: Encoder[Short] = Encoder(a => java.lang.Short.valueOf(a))
  given ByteEncoder: Encoder[Byte] = Encoder(a => java.lang.Byte.valueOf(a))
  given DoubleEncoder: Encoder[Double] = Encoder(a => java.lang.Double.valueOf(a))
  given FloatEncoder: Encoder[Float] = Encoder(a => java.lang.Float.valueOf(a))
  given BooleanEncoder: Encoder[Boolean] = Encoder(a => java.lang.Boolean.valueOf(a))
}

object IntEncoder extends Encoder[Int] {
  override def encode(schema: Schema): Int => AnyRef = { value => java.lang.Integer.valueOf(value) }
}
