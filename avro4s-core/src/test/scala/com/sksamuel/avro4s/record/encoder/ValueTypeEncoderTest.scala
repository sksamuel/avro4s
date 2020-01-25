package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchemaV2, EncoderV2, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ValueTypeEncoderTest extends AnyFunSuite with Matchers {

  test("top level value types") {
    val schema = AvroSchemaV2[FooValueType]
    EncoderV2[FooValueType].encode(FooValueType("hello")) shouldBe new Utf8("hello")
  }

  test("support fields that are value types") {
    case class Test(foo: FooValueType)
    val schema = AvroSchemaV2[Test]
    EncoderV2[Test].encode(Test(FooValueType("hello"))) shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
  }

  test("support value types inside Options") {
    case class Test(foo: Option[FooValueType])
    val schema = AvroSchemaV2[Test]
    val record = EncoderV2[Test].encode(Test(Some(FooValueType("hello"))))
    record shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
  }
}

case class FooValueType(s: String) extends AnyVal
