package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{AvroSchema, Encoder, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.{Matchers, WordSpec}

class ValueTypeEncoderTest extends WordSpec with Matchers {

  "Encoder" should {
    "support fields that are value types" in {
      case class Test(foo: FooValueType)
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(FooValueType("hello")), schema) shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
    }
    "support value types inside Options" in {
      case class Test(foo: Option[FooValueType])
      val schema = AvroSchema[Test]
      val record = Encoder[Test].encode(Test(Some(FooValueType("hello"))), schema)
      record shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
    }
  }
}
case class FooValueType(s: String) extends AnyVal
