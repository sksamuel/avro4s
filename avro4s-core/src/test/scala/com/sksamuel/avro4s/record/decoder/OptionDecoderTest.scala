package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.internal.{Decoder, SchemaFor}
import org.apache.avro.generic.GenericData
import org.scalatest.{Matchers, WordSpec}

case class OptionBoolean(b: Option[Boolean])
case class OptionString(s: Option[String])

class OptionDecoderTest extends WordSpec with Matchers {

  "Decoder" should {
    "support String options" in {
      val schema = SchemaFor[OptionString]

      val record1 = new GenericData.Record(schema)
      record1.put("s", "hello")
      Decoder[OptionString].decode(record1) shouldBe OptionString(Some("hello"))

      val record2 = new GenericData.Record(schema)
      record2.put("s", null)
      Decoder[OptionString].decode(record2) shouldBe OptionString(None)
    }
    "support boolean options" in {
      val schema = SchemaFor[OptionBoolean]

      val record1 = new GenericData.Record(schema)
      record1.put("b", true)
      Decoder[OptionBoolean].decode(record1) shouldBe OptionBoolean(Some(true))

      val record2 = new GenericData.Record(schema)
      record2.put("b", null)
      Decoder[OptionBoolean].decode(record2) shouldBe OptionBoolean(None)
    }
  }
}

