package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{AvroSchema, Encoder, ImmutableRecord, SchemaFor}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class ValueTypeEncoderTest extends FunSuite with Matchers {

  test("top level value types") {
    val schema = AvroSchema[FooValueType]
    Encoder[FooValueType].encode(FooValueType("hello"), schema) shouldBe new Utf8("hello")
  }

  test("support fields that are value types") {
    case class Test(foo: FooValueType)
    val schema = AvroSchema[Test]
    Encoder[Test].encode(Test(FooValueType("hello")), schema) shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
  }

  test("support value types inside Options") {
    case class Test(foo: Option[FooValueType])
    val schema = AvroSchema[Test]
    val record = Encoder[Test].encode(Test(Some(FooValueType("hello"))), schema)
    record shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
  }

  test("support custom typeclasses for nested value types") {

    implicit object FooValueTypeSchemaFor extends SchemaFor[FooValueType] {
      override def schema: Schema = SchemaBuilder.builder().intType()
    }

    implicit object FooValueTypeEncoder extends Encoder[FooValueType] {
      override def encode(t: FooValueType, schema: Schema): AnyRef = java.lang.Integer.valueOf(t.s.toInt)
    }

    case class Test(foo: FooValueType)
    val schema = AvroSchema[Test]
    val record = Encoder[Test].encode(Test(FooValueType("123")), schema)
    record shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(123)))
  }
}

case class FooValueType(s: String) extends AnyVal
