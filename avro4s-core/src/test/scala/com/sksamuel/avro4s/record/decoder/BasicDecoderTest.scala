package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultFieldMapper}
import com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

case class FooString(str: String)
case class FooDouble(d: Double)
case class FooBoolean(b: Boolean)
case class FooFloat(f: Float)
case class FooLong(l: Long)
case class FooInt(i: Int)

class BasicDecoderTest extends AnyWordSpec with Matchers {

  "Decoder" should {
    "decode strings" in {
      val schema = AvroSchema[FooString]
      val record = new GenericData.Record(schema)
      record.put("str", "hello")
      Decoder[FooString].decode(record, schema, DefaultFieldMapper) shouldBe FooString("hello")
    }
    "decode longs" in {
      val schema = AvroSchema[FooLong]
      val record = new GenericData.Record(schema)
      record.put("l", 123456L)
      Decoder[FooLong].decode(record, schema, DefaultFieldMapper) shouldBe FooLong(123456L)
    }
    "decode doubles" in {
      val schema = AvroSchema[FooDouble]
      val record = new GenericData.Record(schema)
      record.put("d", 123.435D)
      Decoder[FooDouble].decode(record, schema, DefaultFieldMapper) shouldBe FooDouble(123.435D)
    }
    "decode booleans" in {
      val schema = AvroSchema[FooBoolean]
      val record = new GenericData.Record(schema)
      record.put("b", true)
      Decoder[FooBoolean].decode(record, schema, DefaultFieldMapper) shouldBe FooBoolean(true)
    }
    "decode floats" in {
      val schema = AvroSchema[FooFloat]
      val record = new GenericData.Record(schema)
      record.put("f", 123.435F)
      Decoder[FooFloat].decode(record, schema, DefaultFieldMapper) shouldBe FooFloat(123.435F)
    }
    "decode ints" in {
      val schema = AvroSchema[FooInt]
      val record = new GenericData.Record(schema)
      record.put("i", 123)
      Decoder[FooInt].decode(record, schema, DefaultFieldMapper) shouldBe FooInt(123)
    }
    "support uppercase packages" in {

      val schema = AvroSchema[ClassInUppercasePackage]
      val decoder = Decoder[ClassInUppercasePackage]

      val record = new GenericData.Record(schema)
      record.put("s", new Utf8("hello"))

      decoder.decode(record, schema, DefaultFieldMapper) shouldBe com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage("hello")
    }
  }
}


