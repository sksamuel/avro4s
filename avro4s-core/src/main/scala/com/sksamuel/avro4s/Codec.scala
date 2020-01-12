package com.sksamuel.avro4s

import magnolia.{CaseClass, Magnolia, SealedTrait}
import org.apache.avro.Schema
import shapeless.{:+:, CNil, Coproduct}

import scala.annotation.implicitNotFound
import scala.reflect.runtime.universe._
import scala.language.experimental.macros

@implicitNotFound(msg = """Unable to build a codec; please check the following:
- an implicit com.sksamuel.avro4s.FieldMapper must be in scope, and
- the given type must be either a case class or a sealed trait with subtypes being
  case classes or case objects.""")
trait Codec[T] extends EncoderV2[T] with DecoderV2[T] {
  self =>

  def schema: Schema

  def encode(value: T): AnyRef

  def decode(value: Any): T

}

object Codec extends ADTCodec with BaseCodecs {

  implicit class CodecBifunctor[T](val codec: Codec[T]) extends AnyVal {
    def inmap[S](f: T => S, g: S => T): Codec[S] = {
      new Codec[S] {
        def schema: Schema = codec.schema

        def encode(value: S): AnyRef = codec.encode(g(value))

        def decode(value: Any): S = f(codec.decode(value))
      }
    }
  }

  implicit def coproductBaseCodec[S: WeakTypeTag: Manifest](implicit codec: Codec[S]): Codec[S :+: CNil] =
    new CoproductBaseCodec(codec)

  implicit def coproductEncoder[H: WeakTypeTag: Manifest, T <: Coproduct](implicit codecH: Codec[H],
                                                                          codecT: Codec[T]): Codec[H :+: T] =
    new CoproductCodec(codecH, codecT)

}
