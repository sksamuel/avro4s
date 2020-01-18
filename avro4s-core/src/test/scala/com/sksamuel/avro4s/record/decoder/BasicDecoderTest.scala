package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage
import com.sksamuel.avro4s.{AvroSchemaV2, DecoderV2, DefaultFieldMapper, FieldMapper}
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

  implicit val fm: FieldMapper = DefaultFieldMapper

  "Decoder" should {
    "decode strings" in {
      val schema = AvroSchemaV2[FooString]
      val record = new GenericData.Record(schema)
      record.put("str", "hello")
      DecoderV2[FooString].decode(record) shouldBe FooString("hello")
    }
    "decode longs" in {
      val schema = AvroSchemaV2[FooLong]
      val record = new GenericData.Record(schema)
      record.put("l", 123456L)
      DecoderV2[FooLong].decode(record) shouldBe FooLong(123456L)
    }
    "decode doubles" in {
      val schema = AvroSchemaV2[FooDouble]
      val record = new GenericData.Record(schema)
      record.put("d", 123.435D)
      DecoderV2[FooDouble].decode(record) shouldBe FooDouble(123.435D)
    }
    "decode booleans" in {
      val schema = AvroSchemaV2[FooBoolean]
      val record = new GenericData.Record(schema)
      record.put("b", true)
      DecoderV2[FooBoolean].decode(record) shouldBe FooBoolean(true)
    }
    "decode floats" in {
      val schema = AvroSchemaV2[FooFloat]
      val record = new GenericData.Record(schema)
      record.put("f", 123.435F)
      DecoderV2[FooFloat].decode(record) shouldBe FooFloat(123.435F)
    }
    "decode ints" in {
      val schema = AvroSchemaV2[FooInt]
      val record = new GenericData.Record(schema)
      record.put("i", 123)
      DecoderV2[FooInt].decode(record) shouldBe FooInt(123)
    }
    "support uppercase packages" in {

      val schema = AvroSchemaV2[ClassInUppercasePackage]
      val decoder = DecoderV2[ClassInUppercasePackage]

      val record = new GenericData.Record(schema)
      record.put("s", new Utf8("hello"))

      decoder.decode(record) shouldBe com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage("hello")
    }
  }
}


