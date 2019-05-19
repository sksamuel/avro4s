package com.sksamuel.avro4s.cats

import cats.data.{NonEmptyList, NonEmptyVector}
import com.sksamuel.avro4s.{DefaultNamingStrategy, Encoder, NamingStrategy}
import org.apache.avro.Schema

import scala.language.implicitConversions

object Encoders {

  import scala.collection.JavaConverters._

  implicit def nonEmptyListEncoder[T](encoder: Encoder[T]) = new Encoder[NonEmptyList[T]] {
    override def encode(ts: NonEmptyList[T], schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): java.util.List[AnyRef] = {
      require(schema != null)
      ts.map(encoder.encode(_, schema.getElementType)).toList.asJava
    }
  }

  implicit def nonEmptyVectorEncoder[T](encoder: Encoder[T]) = new Encoder[NonEmptyVector[T]] {
    override def encode(ts: NonEmptyVector[T], schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): java.util.List[AnyRef] = {
      require(schema != null)
      ts.map(encoder.encode(_, schema.getElementType)).toVector.asJava
    }
  }

}
