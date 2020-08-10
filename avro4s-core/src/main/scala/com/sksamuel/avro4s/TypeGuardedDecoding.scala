package com.sksamuel.avro4s

import com.sksamuel.avro4s.AvroValue.{AvroBoolean, AvroByteArray, AvroDouble, AvroFloat, AvroInt, AvroList, AvroLong, AvroMap, AvroRecord, AvroString}

import scala.reflect.runtime.universe._

object TypeGuardedDecoding {

  def guard[T: WeakTypeTag](decoder: Decoder[T]): PartialFunction[AvroValue, T] = {
    import scala.reflect.runtime.universe.typeOf

    val tpe = implicitly[WeakTypeTag[T]].tpe

    if (tpe <:< typeOf[java.lang.String]) stringDecoder(decoder)
    else if (tpe <:< typeOf[Boolean]) booleanDecoder(decoder)
    else if (tpe <:< typeOf[Int]) intDecoder(decoder)
    else if (tpe <:< typeOf[Long]) longDecoder(decoder)
    else if (tpe <:< typeOf[Double]) doubleDecoder(decoder)
    else if (tpe <:< typeOf[Float]) floatDecoder(decoder)
    else if (tpe <:< typeOf[Array[Byte]]) byteArrayDecoder(decoder)
    else if (tpe <:< typeOf[Array[_]] || tpe <:< typeOf[java.util.Collection[_]] || tpe <:< typeOf[Iterable[_]]) {
      arrayDecoder(decoder)
    } else if (tpe <:< typeOf[java.util.Map[_, _]] || tpe <:< typeOf[Map[_, _]]) {
      mapDecoder(decoder)
    } else if (tpe <:< typeOf[shapeless.Coproduct]) {
      coproductDecoder(decoder)
    } else {
      recordDecoder(decoder.schema.getFullName, decoder)
    }
  }

  private def stringDecoder[T](decoder: Decoder[T]): PartialFunction[AvroValue, T] = {
    case v: AvroString => decoder.decode(v)
  }

  private def booleanDecoder[T](decoder: Decoder[T]): PartialFunction[AvroValue, T] = {
    case b: AvroBoolean => decoder.decode(b)
  }

  private def intDecoder[T](decoder: Decoder[T]): PartialFunction[AvroValue, T] = {
    case i: AvroInt => decoder.decode(i)
  }

  private def longDecoder[T](decoder: Decoder[T]): PartialFunction[AvroValue, T] = {
    case v: AvroLong => decoder.decode(v)
  }

  private def doubleDecoder[T](decoder: Decoder[T]): PartialFunction[AvroValue, T] = {
    case v: AvroDouble => decoder.decode(v)
  }

  private def floatDecoder[T](decoder: Decoder[T]): PartialFunction[AvroValue, T] = {
    case v: AvroFloat => decoder.decode(v)
  }

  private def arrayDecoder[T](decoder: Decoder[T]): PartialFunction[AvroValue, T] = {
    case v: AvroList => decoder.decode(v)
  }

  private def byteArrayDecoder[T](decoder: Decoder[T]): PartialFunction[AvroValue, T] = {
    case v: AvroByteArray => decoder.decode(v)
  }

  private def mapDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
    case v: AvroMap => decoder.decode(v)
  }

  private def coproductDecoder[T](decoder: Decoder[T]): PartialFunction[AvroValue, T] = scala.Function.unlift { value =>
    // this is only sort of safe because we will call TypeGuardedDecoding again in the decoder
    util.Try(decoder.decode(value)) match {
      case util.Success(cp) => Some(cp)
      case _ => None
    }
  }

  private def recordDecoder[T](typeName: String, decoder: Decoder[T]): PartialFunction[AvroValue, T] = {
    case v: AvroRecord if v.record.getSchema.getFullName == typeName => decoder.decode(v)
  }
}
