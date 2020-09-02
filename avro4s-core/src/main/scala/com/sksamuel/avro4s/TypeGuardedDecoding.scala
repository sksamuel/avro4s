package com.sksamuel.avro4s

import java.nio.ByteBuffer

import org.apache.avro.generic.{GenericContainer, GenericFixed}
import org.apache.avro.util.Utf8

import scala.reflect.runtime.universe._

object TypeGuardedDecoding {

  def guard[T: WeakTypeTag](decoder: Decoder[T]): PartialFunction[Any, T] = {
    import scala.reflect.runtime.universe.typeOf

    val tpe = implicitly[WeakTypeTag[T]].tpe

    if (tpe <:< typeOf[java.lang.String]) stringDecoder(decoder)
    else if (tpe <:< typeOf[Boolean]) booleanDecoder(decoder)
    else if (tpe <:< typeOf[Int]) intDecoder(decoder)
    else if (tpe <:< typeOf[Long]) longDecoder(decoder)
    else if (tpe <:< typeOf[Double]) doubleDecoder(decoder)
    else if (tpe <:< typeOf[Float]) floatDecoder(decoder)
    else if (tpe <:< typeOf[Array[Byte]]) byteArrayDecoder(decoder)
    else if (tpe <:< typeOf[java.util.Map[_, _]] || tpe <:< typeOf[Map[_, _]]) {
      mapDecoder(decoder)
    } else if (tpe <:< typeOf[Array[_]] || tpe <:< typeOf[java.util.Collection[_]] || tpe <:< typeOf[Iterable[_]]) {
      arrayDecoder(decoder)
    } else if (tpe <:< typeOf[shapeless.Coproduct]) {
      coproductDecoder(decoder)
    } else {
      recordDecoder(decoder.schema.getFullName, decoder)
    }
  }

  private def stringDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
    case v: Utf8   => decoder.decode(v)
    case v: String => decoder.decode(v)
  }

  private def booleanDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
    case v: Boolean => decoder.decode(v)
  }

  private def intDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
    case v: Int => decoder.decode(v)
  }

  private def longDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
    case v: Long => decoder.decode(v)
  }

  private def doubleDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
    case v: Double => decoder.decode(v)
  }

  private def floatDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
    case v: Float => decoder.decode(v)
  }

  private def arrayDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
    case v: Array[_]                => decoder.decode(v)
    case v: java.util.Collection[_] => decoder.decode(v)
    case v: Iterable[_]             => decoder.decode(v)
  }

  private def byteArrayDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
    case v: ByteBuffer   => decoder.decode(v)
    case v: Array[Byte]  => decoder.decode(v)
    case v: GenericFixed => decoder.decode(v)
  }

  private def mapDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
    case v: java.util.Map[_, _] => decoder.decode(v)
  }

  private def coproductDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = scala.Function.unlift { value =>
    // this is only sort of safe because we will call TypeGuardedDecoding again in the decoder
    util.Try(decoder.decode(value)) match {
      case util.Success(cp) => Some(cp)
      case _                => None
    }
  }

  private def recordDecoder[T](typeName: String, decoder: Decoder[T]): PartialFunction[Any, T] = {
    case v: GenericContainer if v.getSchema.getFullName == typeName => decoder.decode(v)
  }
}
