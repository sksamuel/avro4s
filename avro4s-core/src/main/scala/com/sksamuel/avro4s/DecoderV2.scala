package com.sksamuel.avro4s

import org.apache.avro.Schema

trait DecoderV2[T] {

  def schema: Schema

  def decode(value: Any): T

}

object DecoderV2 {
  implicit class DecoderFunctor[T](val decoder: DecoderV2[T]) extends AnyVal {
    def map[S](f: T => S): DecoderV2[S] = {
      new DecoderV2[S] {
        def schema: Schema = decoder.schema

        def decode(value: Any): S = f(decoder.decode(value))
      }
    }
  }

  def apply[T](implicit codec: Codec[T]): DecoderV2[T] = codec
}
