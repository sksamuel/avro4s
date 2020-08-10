package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.AvroValue.{AvroRecord, AvroString}
import com.sksamuel.avro4s.{AvroSchema, Decoder}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ValueTypeDecoderTest extends AnyFunSuite with Matchers {

  case class Test(foo: FooValueType)
  case class OptionTest(foo: Option[FooValueType])

  test("top level value types") {
    val actual = Decoder[FooValueType].decode(AvroString("hello"))
    actual shouldBe FooValueType("hello")
  }

  test("support fields that are value types") {
    val schema = AvroSchema[Test]

    val record1 = new GenericData.Record(schema)
    record1.put("foo", new Utf8("hello"))

    Decoder[Test].decode(AvroRecord(record1)) shouldBe Test(FooValueType("hello"))
  }

  test("support value types inside Options") {
    val schema = AvroSchema[OptionTest]

    val record1 = new GenericData.Record(schema)
    record1.put("foo", new Utf8("hello"))

    Decoder[OptionTest].decode(AvroRecord(record1)) shouldBe OptionTest(Some(FooValueType("hello")))
  }
}

case class FooValueType(s: String) extends AnyVal
