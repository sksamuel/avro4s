package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, DefaultFieldMapper, Encoder, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class ValueTypeEncoderTest extends FunSuite with Matchers {

  test("top level value types") {
    val schema = AvroSchema[FooValueType]
    Encoder[FooValueType].encode(FooValueType("hello"), schema, DefaultFieldMapper) shouldBe new Utf8("hello")
  }

  test("support fields that are value types") {
    case class Test(foo: FooValueType)
    val schema = AvroSchema[Test]
    Encoder[Test].encode(Test(FooValueType("hello")), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
  }

  test("support value types inside Options") {
    case class Test(foo: Option[FooValueType])
    val schema = AvroSchema[Test]
    val record = Encoder[Test].encode(Test(Some(FooValueType("hello"))), schema, DefaultFieldMapper)
    record shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
  }
}

case class FooValueType(s: String) extends AnyVal
