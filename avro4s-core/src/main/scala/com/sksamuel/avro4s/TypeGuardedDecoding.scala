package com.sksamuel.avro4s

import org.apache.avro.Schema

import java.nio.ByteBuffer
import org.apache.avro.generic.{GenericContainer, GenericFixed}
import org.apache.avro.util.Utf8

import java.util.UUID

trait TypeGuardedDecoding[T] extends Serializable {
  def guard(schema: Schema): PartialFunction[Any, Boolean]
}

object TypeGuardedDecoding {

  given TypeGuardedDecoding[String] = new TypeGuardedDecoding[String] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Utf8 => true
      case v: String => true
    }

  given TypeGuardedDecoding[Boolean] = new TypeGuardedDecoding[Boolean] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Boolean => true
    }

  given TypeGuardedDecoding[Double] = new TypeGuardedDecoding[Double] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Double => true
      case v: Float => true
    }

  given TypeGuardedDecoding[Float] = new TypeGuardedDecoding[Float] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Float => true
    }

  given TypeGuardedDecoding[Long] = new TypeGuardedDecoding[Long] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Long => true
      case v: Int => true
      case v: Short => true
      case v: Byte => true
    }

  given TypeGuardedDecoding[Int] = new TypeGuardedDecoding[Int] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: Int => true
    }

  given[T]: TypeGuardedDecoding[T] = new TypeGuardedDecoding[T] :
    override def guard(schema: Schema): PartialFunction[Any, Boolean] = {
      case v: GenericContainer if v.getSchema.getFullName == schema.getFullName => true
    }
}

//  private[this] final val StringType = typeOf[String]
//  private[this] final val UUIDType = typeOf[UUID]
//  private[this] final val ByteArrayType = typeOf[Array[Byte]]
//  private[this] final val JavaMapType = typeOf[java.util.Map[_, _]]
//  private[this] final val MapType = typeOf[Map[_, _]]
//  private[this] final val ArrayType = typeOf[Array[_]]
//  private[this] final val JavaCollectionType = typeOf[java.util.Collection[_]]
//  private[this] final val IterableType = typeOf[Iterable[_]]
//  private[this] final val CoproductType = typeOf[shapeless.Coproduct]
//
//  def apply[T](implicit ev: TypeGuardedDecoding[T]): TypeGuardedDecoding[T] = ev
//
//  implicit final def derive[T: WeakTypeTag]: TypeGuardedDecoding[T] = new TypeGuardedDecoding[T] {
//    def guard(decoder: Decoder[T]): PartialFunction[Any, T] = {
//      val tpe = implicitly[WeakTypeTag[T]].tpe
//
//      if (tpe <:< StringType) stringDecoder(decoder)
//      else if (tpe <:< UUIDType) uuidDecoder(decoder)
//      else if (tpe <:< WeakTypeTag.Boolean.tpe) booleanDecoder(decoder)
//      else if (tpe <:< WeakTypeTag.Int.tpe) intDecoder(decoder)
//      else if (tpe <:< WeakTypeTag.Long.tpe) longDecoder(decoder)
//      else if (tpe <:< WeakTypeTag.Double.tpe) doubleDecoder(decoder)
//      else if (tpe <:< WeakTypeTag.Float.tpe) floatDecoder(decoder)
//      else if (tpe <:< ByteArrayType) byteArrayDecoder(decoder)
//      else if (tpe <:< JavaMapType || tpe <:< MapType) mapDecoder(decoder)
//      else if (tpe <:< ArrayType || tpe <:< JavaCollectionType || tpe <:< IterableType) arrayDecoder(decoder)
//      else if (tpe <:< CoproductType) coproductDecoder(decoder)
//      else recordDecoder(decoder.schema.getFullName, decoder)
//    }
//  }
//
//  private def stringDecoder[T](decoder: Decoder[T])
//
//  private def uuidDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
//    case v: Utf8 => decoder.decode(v)
//    case v: String => decoder.decode(v)
//  }
//
//  private def arrayDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
//    case v: Array[_] => decoder.decode(v)
//    case v: java.util.Collection[_] => decoder.decode(v)
//    case v: Iterable[_] => decoder.decode(v)
//  }
//
//  private def byteArrayDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
//    case v: ByteBuffer => decoder.decode(v)
//    case v: Array[Byte] => decoder.decode(v)
//    case v: GenericFixed => decoder.decode(v)
//  }
//
//  private def mapDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = {
//    case v: java.util.Map[_, _] => decoder.decode(v)
//  }
//
//  private def coproductDecoder[T](decoder: Decoder[T]): PartialFunction[Any, T] = scala.Function.unlift { value =>
//    // this is only sort of safe because we will call TypeGuardedDecoding again in the decoder
//    util.Try(decoder.decode(value)) match {
//      case util.Success(cp) => Some(cp)
//      case _ => None
//    }
//  }
//

//}
