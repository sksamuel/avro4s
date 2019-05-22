package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultNamingStrategy}
import org.apache.avro.generic.GenericData
import org.scalatest.{Matchers, WordSpec}

case class OptionBoolean(b: Option[Boolean])
case class OptionString(s: Option[String])
case class RequiredString(s: String)

class OptionDecoderTest extends WordSpec with Matchers {

  "Decoder" should {
    "support String options" in {
      val schema = AvroSchema[OptionString]

      val record1 = new GenericData.Record(schema)
      record1.put("s", "hello")
      Decoder[OptionString].decode(record1, schema, DefaultNamingStrategy) shouldBe OptionString(Some("hello"))

      val record2 = new GenericData.Record(schema)
      record2.put("s", null)
      Decoder[OptionString].decode(record2, schema, DefaultNamingStrategy) shouldBe OptionString(None)
    }
    "support decoding required fields as Option" in {
      val requiredStringSchema = AvroSchema[RequiredString]

      val requiredStringRecord = new GenericData.Record(requiredStringSchema)
      requiredStringRecord.put("s", "hello")
      Decoder[OptionString].decode(requiredStringRecord, requiredStringSchema, DefaultNamingStrategy) shouldBe OptionString(Some("hello"))
    }
    "support boolean options" in {
      val schema = AvroSchema[OptionBoolean]

      val record1 = new GenericData.Record(schema)
      record1.put("b", true)
      Decoder[OptionBoolean].decode(record1, schema, DefaultNamingStrategy) shouldBe OptionBoolean(Some(true))

      val record2 = new GenericData.Record(schema)
      record2.put("b", null)
      Decoder[OptionBoolean].decode(record2, schema, DefaultNamingStrategy) shouldBe OptionBoolean(None)
    }
  }
}

