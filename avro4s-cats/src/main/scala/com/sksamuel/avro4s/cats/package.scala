package com.sksamuel.avro4s

import _root_.cats.data.{NonEmptyList, NonEmptyVector}
import org.apache.avro.Schema

import scala.language.implicitConversions

package object cats {

  import scala.collection.JavaConverters._

  implicit def nonEmptyListSchemaFor[T](implicit schemaFor: SchemaFor[T]): SchemaFor[NonEmptyList[T]] =
    SchemaFor(Schema.createArray(schemaFor.schema))

  implicit def nonEmptyVectorSchemaFor[T](implicit schemaFor: SchemaFor[T]): SchemaFor[NonEmptyVector[T]] =
    SchemaFor(Schema.createArray(schemaFor.schema))

  implicit def nonEmptyListEncoder[T](implicit encoder: Encoder[T]) = new Encoder[NonEmptyList[T]] {

    val schemaFor: SchemaFor[NonEmptyList[T]] = nonEmptyListSchemaFor(encoder.schemaFor)

    override def encode(ts: NonEmptyList[T]): java.util.List[AnyRef] = {
      require(schema != null)
      ts.map(encoder.encode).toList.asJava
    }
  }

  implicit def nonEmptyVectorEncoder[T](implicit encoder: Encoder[T]) = new Encoder[NonEmptyVector[T]] {

    val schemaFor: SchemaFor[NonEmptyVector[T]] = nonEmptyVectorSchemaFor(encoder.schemaFor)

    override def encode(ts: NonEmptyVector[T]): java.util.List[AnyRef] = {
      require(schema != null)
      ts.map(encoder.encode).toVector.asJava
    }
  }

  implicit def nonEmptyListDecoder[T](implicit decoder: Decoder[T]) = new Decoder[NonEmptyList[T]] {

    val schemaFor: SchemaFor[NonEmptyList[T]] = nonEmptyListSchemaFor(decoder.schemaFor)

    override def decode(value: Any): NonEmptyList[T] = value match {
      case array: Array[_] => NonEmptyList.fromListUnsafe(array.toList.map(decoder.decode))
      case list: java.util.Collection[_] => NonEmptyList.fromListUnsafe(list.asScala.map(decoder.decode).toList)
      case other => sys.error("Unsupported type " + other)
    }
  }

  implicit def nonEmptyVectorDecoder[T](implicit decoder: Decoder[T]) = new Decoder[NonEmptyVector[T]] {

    val schemaFor: SchemaFor[NonEmptyVector[T]] = nonEmptyVectorSchemaFor(decoder.schemaFor)

    override def decode(value: Any): NonEmptyVector[T] = value match {
      case array: Array[_] => NonEmptyVector.fromVectorUnsafe(array.toVector.map(decoder.decode))
      case list: java.util.Collection[_] => NonEmptyVector.fromVectorUnsafe(list.asScala.map(decoder.decode).toVector)
      case other => sys.error("Unsupported type " + other)
    }
  }
}
