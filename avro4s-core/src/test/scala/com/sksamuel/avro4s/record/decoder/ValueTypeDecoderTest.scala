package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, SchemaFor}
import com.sksamuel.avro4s.Decoder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{FunSuite, Matchers}

class ValueTypeDecoderTest extends FunSuite with Matchers {

  case class Test(foo: FooValueType)
  case class OptionTest(foo: Option[FooValueType])

  test("top level value types") {
    val actual = Decoder[FooValueType].decode("hello", AvroSchema[FooValueType])
    actual shouldBe FooValueType("hello")
  }

  test("support fields that are value types") {
    val schema = AvroSchema[Test]

    val record1 = new GenericData.Record(schema)
    record1.put("foo", new Utf8("hello"))

    Decoder[Test].decode(record1, schema) shouldBe Test(FooValueType("hello"))
  }

  test("support value types inside Options") {
    val schema = AvroSchema[OptionTest]

    val record1 = new GenericData.Record(schema)
    record1.put("foo", new Utf8("hello"))

    Decoder[OptionTest].decode(record1, schema) shouldBe OptionTest(Some(FooValueType("hello")))
  }

  test("support custom typeclasses for nested value types") {

    implicit object FooValueTypeSchemaFor extends SchemaFor[FooValueType] {
      override def schema: Schema = SchemaBuilder.builder().intType()
    }

    implicit object FooValueTypeDecoder extends Decoder[FooValueType] {
      override def decode(value: Any, schema: Schema): FooValueType = FooValueType(Decoder.IntDecoder.map(_.toString).decode(value, AvroSchema[FooValueType]))
    }

    val schema = AvroSchema[Test]
    val record = new GenericData.Record(schema)
    record.put("foo", 123)

    Decoder[Test].decode(record, schema) shouldBe Test(FooValueType("123"))
  }
}

case class FooValueType(s: String) extends AnyVal
