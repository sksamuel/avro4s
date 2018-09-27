package com.sksamuel.avro4s.cats

import cats.data.{NonEmptyList, NonEmptyVector}
import com.sksamuel.avro4s.internal.Decoder

import scala.language.implicitConversions

object Decoders {

  import scala.collection.JavaConverters._

  implicit def nonEmptyListEncoder[T](decoder: Decoder[T]) = new Decoder[NonEmptyList[T]] {
    override def decode(value: Any): NonEmptyList[T] = value match {
      case array: Array[_] => NonEmptyList.fromListUnsafe(array.map(decoder.decode).toList)
      case list: java.util.Collection[_] => NonEmptyList.fromListUnsafe(list.asScala.map(decoder.decode).toList)
      case other => sys.error("Unsupported type " + other)
    }
  }

  implicit def nonEmptyVectorEncoder[T](decoder: Decoder[T]) = new Decoder[NonEmptyVector[T]] {
    override def decode(value: Any): NonEmptyVector[T] = value match {
      case array: Array[_] => NonEmptyVector.fromVectorUnsafe(array.map(decoder.decode).toVector)
      case list: java.util.Collection[_] => NonEmptyVector.fromVectorUnsafe(list.asScala.map(decoder.decode).toVector)
      case other => sys.error("Unsupported type " + other)
    }
  }
}
