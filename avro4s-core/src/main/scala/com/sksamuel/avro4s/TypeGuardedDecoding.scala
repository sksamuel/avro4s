package com.sksamuel.avro4s

import org.apache.avro.generic.{GenericContainer, GenericData}
import org.apache.avro.util.Utf8
import scala.reflect.runtime.universe._

object TypeGuardedDecoding {

  def guard[T: WeakTypeTag: Manifest](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    import scala.reflect.runtime.universe.typeOf

    val tpe = implicitly[WeakTypeTag[T]].tpe

    if (tpe <:< typeOf[java.lang.String]) stringDecoder(decoder)
    else if (tpe <:< typeOf[Boolean]) booleanDecoder(decoder)
    else if (tpe <:< typeOf[Int]) intDecoder(decoder)
    else if (tpe <:< typeOf[Long]) longDecoder(decoder)
    else if (tpe <:< typeOf[Double]) doubleDecoder(decoder)
    else if (tpe <:< typeOf[Float]) floatDecoder(decoder)
    else if (tpe <:< typeOf[Array[_]] || tpe <:< typeOf[java.util.Collection[_]] || tpe <:< typeOf[Iterable[_]]) {
      arrayDecoder(decoder)
    } else if (tpe <:< typeOf[java.util.Map[_, _]] || tpe <:< typeOf[Map[_, _]]) {
      mapDecoder(decoder)
    } else {
      val nameExtractor = NameExtractor(manifest.runtimeClass)
      recordDecoder(nameExtractor.fullName, decoder)
    }
  }

  private def stringDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: Utf8   => decoder.decode(v)
    case v: String => decoder.decode(v)
  }

  private def booleanDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: Boolean => decoder.decode(v)
  }

  private def intDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: Int => decoder.decode(v)
  }

  private def longDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: Long => decoder.decode(v)
  }

  private def doubleDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: Double => decoder.decode(v)
  }

  private def floatDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: Float => decoder.decode(v)
  }

  private def arrayDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: GenericData.Array[_] => decoder.decode(v)
  }

  private def mapDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: java.util.Map[_, _] => decoder.decode(v)
  }

  private def recordDecoder[T](typeName: String, decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: GenericContainer if v.getSchema.getFullName == typeName => decoder.decode(v)
  }
}
