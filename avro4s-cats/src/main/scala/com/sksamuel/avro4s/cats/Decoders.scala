package com.sksamuel.avro4s.cats

import cats.data.{NonEmptyList, NonEmptyVector}
import com.sksamuel.avro4s.Decoder
import org.apache.avro.Schema

import scala.language.implicitConversions

object Decoders {

  import scala.collection.JavaConverters._

  implicit def nonEmptyListEncoder[T](decoder: Decoder[T]) = new Decoder[NonEmptyList[T]] {
    override def decode(value: Any, schema: Schema): NonEmptyList[T] = value match {
      case array: Array[_] => NonEmptyList.fromListUnsafe(array.map(decoder.decode(_, schema)).toList)
      case list: java.util.Collection[_] => NonEmptyList.fromListUnsafe(list.asScala.map(decoder.decode(_, schema)).toList)
      case other => sys.error("Unsupported type " + other)
    }
  }

  implicit def nonEmptyVectorEncoder[T](decoder: Decoder[T]) = new Decoder[NonEmptyVector[T]] {
    override def decode(value: Any, schema: Schema): NonEmptyVector[T] = value match {
      case array: Array[_] => NonEmptyVector.fromVectorUnsafe(array.map(decoder.decode(_, schema)).toVector)
      case list: java.util.Collection[_] => NonEmptyVector.fromVectorUnsafe(list.asScala.map(decoder.decode(_, schema)).toVector)
      case other => sys.error("Unsupported type " + other)
    }
  }
}
