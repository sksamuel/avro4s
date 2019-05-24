package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultNamingStrategy}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
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
}

case class FooValueType(s: String) extends AnyVal
