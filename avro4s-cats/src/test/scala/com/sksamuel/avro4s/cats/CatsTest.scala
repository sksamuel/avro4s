package com.sksamuel.avro4s.cats

import cats.data.NonEmptyList
import com.sksamuel.avro4s.{AvroSchema, AvroSchemaV2, FieldMapper, SchemaFor}
import org.apache.avro.Schema
import org.scalatest.{FunSuite, Matchers}

import scala.language.implicitConversions

case class Foo(list: NonEmptyList[String])

class CatsTest extends FunSuite with Matchers {

  implicit def nonEmptyListSchemaFor[T](schemaFor: SchemaFor[T]): SchemaFor[NonEmptyList[T]] = {
    new SchemaFor[NonEmptyList[T]] {
      override def schema(fieldMapper: FieldMapper): Schema = Schema.createArray(schemaFor.schema(fieldMapper))
    }
  }

  ignore("cats") {
    AvroSchemaV2[Foo] shouldBe """"""
  }
}
