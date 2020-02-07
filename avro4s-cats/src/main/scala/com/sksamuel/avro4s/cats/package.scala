package com.sksamuel.avro4s

import _root_.cats.data.{NonEmptyList, NonEmptyVector}
import org.apache.avro.Schema

import scala.language.implicitConversions

package object cats {

  import scala.collection.JavaConverters._

  implicit def nonEmptyListSchemaFor[T](schemaFor: SchemaFor[T]): SchemaFor[NonEmptyList[T]] = {
    new SchemaFor[NonEmptyList[T]] {
      override def schema(fieldMapper: FieldMapper, context: Context): Schema = Schema.createArray(schemaFor.schema(fieldMapper, context))
    }
  }

  implicit def nonEmptyVectorSchemaFor[T](schemaFor: SchemaFor[T]): SchemaFor[NonEmptyVector[T]] = {
    new SchemaFor[NonEmptyVector[T]] {
      override def schema(fieldMapper: FieldMapper, context: Context): Schema = Schema.createArray(schemaFor.schema(fieldMapper, context))
    }
  }

  implicit def nonEmptyListEncoder[T](encoder: Encoder[T]) = new Encoder[NonEmptyList[T]] {
    override def encode(ts: NonEmptyList[T], schema: Schema, fieldMapper: FieldMapper): java.util.List[AnyRef] = {
      require(schema != null)
      ts.map(encoder.encode(_, schema.getElementType, fieldMapper)).toList.asJava
    }
  }

  implicit def nonEmptyVectorEncoder[T](encoder: Encoder[T]) = new Encoder[NonEmptyVector[T]] {
    override def encode(ts: NonEmptyVector[T], schema: Schema, fieldMapper: FieldMapper): java.util.List[AnyRef] = {
      require(schema != null)
      ts.map(encoder.encode(_, schema.getElementType, fieldMapper)).toVector.asJava
    }
  }

  implicit def nonEmptyListDecoder[T](decoder: Decoder[T]) = new Decoder[NonEmptyList[T]] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): NonEmptyList[T] = value match {
      case array: Array[_] =>
        val list = array.toList.map(decoder.decode(_, schema, fieldMapper))
        NonEmptyList.fromListUnsafe(list)
      case list: java.util.Collection[_] => NonEmptyList.fromListUnsafe(list.asScala.map(decoder.decode(_, schema, fieldMapper)).toList)
      case other => sys.error("Unsupported type " + other)
    }
  }

  implicit def nonEmptyVectorDecoder[T](decoder: Decoder[T]) = new Decoder[NonEmptyVector[T]] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): NonEmptyVector[T] = value match {
      case array: Array[_] => NonEmptyVector.fromVectorUnsafe(array.toVector.map(decoder.decode(_, schema, fieldMapper)))
      case list: java.util.Collection[_] => NonEmptyVector.fromVectorUnsafe(list.asScala.map(decoder.decode(_, schema, fieldMapper)).toVector)
      case other => sys.error("Unsupported type " + other)
    }
  }
}
