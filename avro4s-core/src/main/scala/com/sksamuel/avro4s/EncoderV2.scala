package com.sksamuel.avro4s

import org.apache.avro.Schema

trait EncoderV2[T] {
  def schema: Schema

  def encode(value: T): AnyRef
}

object EncoderV2 {
  implicit class EncoderCofunctor[T](val encoder: EncoderV2[T]) extends AnyVal {
    def comap[S](f: S => T): EncoderV2[S] = {
      new EncoderV2[S] {
        def schema: Schema = encoder.schema

        def encode(value: S): AnyRef = encoder.encode(f(value))
      }
    }
  }

  def apply[T](implicit codec: Codec[T]): EncoderV2[T] = codec
}
