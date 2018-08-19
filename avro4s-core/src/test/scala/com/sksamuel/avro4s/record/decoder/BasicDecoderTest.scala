package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.internal.{Decoder, SchemaEncoder}
import org.apache.avro.generic.GenericData
import org.scalatest.{Matchers, WordSpec}

case class FooString(str: String)
case class FooDouble(d: Double)
case class FooBoolean(b: Boolean)
case class FooFloat(f: Float)
case class FooLong(l: Long)
case class FooInt(i: Int)

class BasicDecoderTest extends WordSpec with Matchers {

  "Decoder" should {
    "decode strings" in {
      val schema = SchemaEncoder[FooString].encode()
      val record = new GenericData.Record(schema)
      record.put("str", "hello")
      Decoder[FooString].decode(record) shouldBe FooString("hello")
    }
    "decode longs" in {
      val schema = SchemaEncoder[FooLong].encode()
      val record = new GenericData.Record(schema)
      record.put("l", 123456L)
      Decoder[FooLong].decode(record) shouldBe FooLong(123456L)
    }
    "encode doubles" in {
      val schema = SchemaEncoder[FooDouble].encode()
      val record = new GenericData.Record(schema)
      record.put("d", 123.435D)
      Decoder[FooDouble].decode(record) shouldBe FooDouble(123.435D)
    }
    "encode booleans" in {
      val schema = SchemaEncoder[FooBoolean].encode()
      val record = new GenericData.Record(schema)
      record.put("b", true)
      Decoder[FooBoolean].decode(record) shouldBe FooBoolean(true)
    }
    "encode floats" in {
      val schema = SchemaEncoder[FooFloat].encode()
      val record = new GenericData.Record(schema)
      record.put("f", 123.435F)
      Decoder[FooFloat].decode(record) shouldBe FooFloat(123.435F)
    }
    "encode ints" in {
      val schema = SchemaEncoder[FooInt].encode()
      val record = new GenericData.Record(schema)
      record.put("i", 123)
      Decoder[FooInt].decode(record) shouldBe FooInt(123)
    }
  }
}


