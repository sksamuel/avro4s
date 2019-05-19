package com.sksamuel.avro4s

import cats.data.{NonEmptyList, NonEmptyVector}
import org.apache.avro.Schema

import scala.language.implicitConversions

object Cats {

  import scala.collection.JavaConverters._

  implicit def nonEmptyListSchemaFor[T](schemaFor: SchemaFor[T]): SchemaFor[NonEmptyList[T]] = {
    new SchemaFor[NonEmptyList[T]] {
      override def schema(implicit namingStrategy: NamingStrategy) = Schema.createArray(schemaFor.schema)
    }
  }

  implicit def nonEmptyVectorSchemaFor[T](schemaFor: SchemaFor[T]): SchemaFor[NonEmptyVector[T]] = {
    new SchemaFor[NonEmptyVector[T]] {
      override def schema(implicit namingStrategy: NamingStrategy) = Schema.createArray(schemaFor.schema)
    }
  }

  implicit def nonEmptyListEncoder[T](encoder: Encoder[T]) = new Encoder[NonEmptyList[T]] {
    override def encode(ts: cats.data.NonEmptyList[T], schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): java.util.List[AnyRef] = {
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

  implicit def nonEmptyListDecoder[T](decoder: Decoder[T]) = new Decoder[NonEmptyList[T]] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): NonEmptyList[T] = value match {
      case array: Array[_] =>
        val list = array.map(decoder.decode(_, schema)).toList
        NonEmptyList.fromListUnsafe(list)
      case list: java.util.Collection[_] => NonEmptyList.fromListUnsafe(list.asScala.map(decoder.decode(_, schema)).toList)
      case other => sys.error("Unsupported type " + other)
    }
  }

  implicit def nonEmptyVectorDecoder[T](decoder: Decoder[T]) = new Decoder[NonEmptyVector[T]] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): NonEmptyVector[T] = value match {
      case array: Array[_] => NonEmptyVector.fromVectorUnsafe(array.toVector.map(decoder.decode(_, schema)))
      case list: java.util.Collection[_] => NonEmptyVector.fromVectorUnsafe(list.asScala.map(decoder.decode(_, schema)).toVector)
      case other => sys.error("Unsupported type " + other)
    }
  }
}
