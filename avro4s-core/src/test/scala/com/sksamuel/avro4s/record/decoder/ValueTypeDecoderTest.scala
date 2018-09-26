package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.internal.{AvroSchema, Decoder}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{Matchers, WordSpec}

class ValueTypeDecoderTest extends WordSpec with Matchers {

  case class Test(foo: FooValueType)
  case class OptionTest(foo: Option[FooValueType])

  "Decoder" should {
    "support fields that are value types" in {
      val schema = AvroSchema[Test]

      val record1 = new GenericData.Record(schema)
      record1.put("foo", new Utf8("hello"))

      Decoder[Test].decode(record1) shouldBe Test(FooValueType("hello"))
    }
    "support value types inside Options" in {
     val schema = AvroSchema[OptionTest]

      val record1 = new GenericData.Record(schema)
      record1.put("foo", new Utf8("hello"))

      Decoder[OptionTest].decode(record1) shouldBe OptionTest(Some(FooValueType("hello")))
    }
  }
}
case class FooValueType(s: String) extends AnyVal
