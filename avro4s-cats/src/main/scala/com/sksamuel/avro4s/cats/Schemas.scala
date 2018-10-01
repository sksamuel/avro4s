package com.sksamuel.avro4s.cats

import cats.data.{NonEmptyList, NonEmptyVector}
import com.sksamuel.avro4s.internal.SchemaFor
import org.apache.avro.Schema

import scala.language.implicitConversions

object Schemas {

  implicit def nonEmptyListSchemaFor[T](schemaFor: SchemaFor[T]): SchemaFor[NonEmptyList[T]] = {
    new SchemaFor[NonEmptyList[T]] {
      override def schema: Schema = Schema.createArray(schemaFor.schema)
    }
  }

  implicit def nonEmptyVectorSchemaFor[T](schemaFor: SchemaFor[T]): SchemaFor[NonEmptyVector[T]] = {
    new SchemaFor[NonEmptyVector[T]] {
      override def schema: Schema = Schema.createArray(schemaFor.schema)
    }
  }
}
