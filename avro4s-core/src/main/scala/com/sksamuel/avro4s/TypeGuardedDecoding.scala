package com.sksamuel.avro4s

import java.nio.ByteBuffer
import org.apache.avro.generic.{GenericContainer, GenericFixed}
import org.apache.avro.util.Utf8

import java.util.UUID
import scala.reflect.runtime.universe._

trait TypeGuardedDecoding[T] extends Serializable {
  def guard(decoderT: Decoder[T]): PartialFunction[Any, T]
}

object TypeGuardedDecoding {
  private[this] final val StringType = typeOf[String]
  private[this] final val UUIDType = typeOf[UUID]
  private[this] final val ByteArrayType = typeOf[Array[Byte]]
  private[this] final val JavaMapType = typeOf[java.util.Map[_, _]]
  private[this] final val MapType = typeOf[Map[_, _]]
  private[this] final val ArrayType = typeOf[Array[_]]
  private[this] final val JavaCollectionType = typeOf[java.util.Collection[_]]
  private[this] final val IterableType = typeOf[Iterable[_]]
  private[this] final val CoproductType = typeOf[shapeless.Coproduct]

  def apply[T](implicit ev: TypeGuardedDecoding[T]): TypeGuardedDecoding[T] = ev

  implicit final def derive[T: WeakTypeTag]: TypeGuardedDecoding[T] = new TypeGuardedDecoding[T] {
    def guard(decoder: Decoder[T]): PartialFunction[Any, T] = {
      val tpe = implicitly[WeakTypeTag[T]].tpe

      if (tpe <:< StringType) stringDecoder(decoder)
      if (tpe <:< UUIDType) uuidDecoder(decoder)
      else if (tpe <:< WeakTypeTag.Boolean.tpe) booleanDecoder(decoder)
      else if (tpe <:< WeakTypeTag.Int.tpe) intDecoder(decoder)
      else if (tpe <:< WeakTypeTag.Long.tpe) longDecoder(decoder)
      else if (tpe <:< WeakTypeTag.Double.tpe) doubleDecoder(decoder)
      else if (tpe <:< WeakTypeTag.Float.tpe) floatDecoder(decoder)
      else if (tpe <:< ByteArrayType) byteArrayDecoder(decoder)
      else if (tpe <:< JavaMapType || tpe <:< MapType) mapDecoder(decoder)
      else if (tpe <:< ArrayType || tpe <:< JavaCollectionType || tpe <:< IterableType) arrayDecoder(decoder)
      else if (tpe <:< CoproductType) coproductDecoder(decoder)
      else recordDecoder(decoder.schema.getFullName, decoder)
    }
  }

  private def stringDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
    case v: Utf8   => decoder.decode(v)
    case v: String => decoder.decode(v)
  }

  private def uuidDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
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
