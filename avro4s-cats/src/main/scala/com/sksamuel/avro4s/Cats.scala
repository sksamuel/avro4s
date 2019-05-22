package com.sksamuel.avro4s

import cats.data.{NonEmptyList, NonEmptyVector}
import org.apache.avro.Schema

import scala.language.implicitConversions

object Cats {

  import scala.collection.JavaConverters._

  implicit def nonEmptyListSchemaFor[T](schemaFor: SchemaFor[T]): SchemaFor[NonEmptyList[T]] = {
    new SchemaFor[NonEmptyList[T]] {
      override def schema(namingStrategy: NamingStrategy) = Schema.createArray(schemaFor.schema(namingStrategy))
    }
  }

  implicit def nonEmptyVectorSchemaFor[T](schemaFor: SchemaFor[T]): SchemaFor[NonEmptyVector[T]] = {
    new SchemaFor[NonEmptyVector[T]] {
      override def schema(namingStrategy: NamingStrategy) = Schema.createArray(schemaFor.schema(namingStrategy))
    }
  }

  implicit def nonEmptyListEncoder[T](encoder: Encoder[T]) = new Encoder[NonEmptyList[T]] {
    override def encode(ts: cats.data.NonEmptyList[T], schema: Schema, naming: NamingStrategy): java.util.List[AnyRef] = {
      require(schema != null)
      ts.map(encoder.encode(_, schema.getElementType, naming)).toList.asJava
    }
  }

  implicit def nonEmptyVectorEncoder[T](encoder: Encoder[T]) = new Encoder[NonEmptyVector[T]] {
    override def encode(ts: NonEmptyVector[T], schema: Schema, naming: NamingStrategy): java.util.List[AnyRef] = {
      require(schema != null)
      ts.map(encoder.encode(_, schema.getElementType, naming)).toVector.asJava
    }
  }

  implicit def nonEmptyListDecoder[T](decoder: Decoder[T]) = new Decoder[NonEmptyList[T]] {
    override def decode(value: Any, schema: Schema, naming: NamingStrategy): NonEmptyList[T] = value match {
      case array: Array[_] =>
        val list = array.map(decoder.decode(_, schema, naming)).toList
        NonEmptyList.fromListUnsafe(list)
      case list: java.util.Collection[_] => NonEmptyList.fromListUnsafe(list.asScala.map(decoder.decode(_, schema, naming)).toList)
      case other => sys.error("Unsupported type " + other)
    }
  }

  implicit def nonEmptyVectorDecoder[T](decoder: Decoder[T]) = new Decoder[NonEmptyVector[T]] {
    override def decode(value: Any, schema: Schema, naming: NamingStrategy): NonEmptyVector[T] = value match {
      case array: Array[_] => NonEmptyVector.fromVectorUnsafe(array.toVector.map(decoder.decode(_, schema, naming)))
      case list: java.util.Collection[_] => NonEmptyVector.fromVectorUnsafe(list.asScala.map(decoder.decode(_, schema, naming)).toVector)
      case other => sys.error("Unsupported type " + other)
    }
  }
}
