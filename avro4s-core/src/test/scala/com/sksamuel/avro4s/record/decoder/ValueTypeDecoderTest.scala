package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchemaV2, DecoderV2}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ValueTypeDecoderTest extends AnyFunSuite with Matchers {

  case class Test(foo: FooValueType)
  case class OptionTest(foo: Option[FooValueType])

  test("top level value types") {
    val actual = DecoderV2[FooValueType].decode("hello")
    actual shouldBe FooValueType("hello")
  }

  test("support fields that are value types") {
    val schema = AvroSchemaV2[Test]

    val record1 = new GenericData.Record(schema)
    record1.put("foo", new Utf8("hello"))

    DecoderV2[Test].decode(record1) shouldBe Test(FooValueType("hello"))
  }

  test("support value types inside Options") {
    val schema = AvroSchemaV2[OptionTest]

    val record1 = new GenericData.Record(schema)
    record1.put("foo", new Utf8("hello"))

    DecoderV2[OptionTest].decode(record1) shouldBe OptionTest(Some(FooValueType("hello")))
  }
}

case class FooValueType(s: String) extends AnyVal
