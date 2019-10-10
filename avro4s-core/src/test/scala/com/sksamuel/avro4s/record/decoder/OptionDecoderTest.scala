package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultFieldMapper, ToRecord}
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}
import org.scalatest.{Matchers, WordSpec}

case class OptionBoolean(b: Option[Boolean])
case class OptionString(s: Option[String])
case class RequiredString(s: String)


sealed trait CupcatOptionEnum
case object CuppersOptionEnum extends CupcatOptionEnum
case object SnoutleyOptionEnum extends CupcatOptionEnum

case class OptionStringDefault(s: Option[String] = Option("cupcat"))
case class OptionEnumDefault(s: Option[CupcatOptionEnum] = Option(CuppersOptionEnum))

case class OptionEnumDefaultWithNone(s: Option[CupcatOptionEnum] = Option(CuppersOptionEnum), t: String)
case class OptionStringDefaultWithNone(s: Option[String] = Option("cupcat"), t: String)

case class SchemaWithoutExpectedField(t: String)

class OptionDecoderTest extends WordSpec with Matchers {

  "Decoder" should {
    "support String options" in {
      val schema = AvroSchema[OptionString]

      val record1 = new GenericData.Record(schema)
      record1.put("s", "hello")
      Decoder[OptionString].decode(record1, schema, DefaultFieldMapper) shouldBe OptionString(Some("hello"))

      val record2 = new GenericData.Record(schema)
      record2.put("s", null)
      Decoder[OptionString].decode(record2, schema, DefaultFieldMapper) shouldBe OptionString(None)
    }
    "support decoding required fields as Option" in {
      val requiredStringSchema = AvroSchema[RequiredString]

      val requiredStringRecord = new GenericData.Record(requiredStringSchema)
      requiredStringRecord.put("s", "hello")
      Decoder[OptionString].decode(requiredStringRecord, requiredStringSchema, DefaultFieldMapper) shouldBe OptionString(Some("hello"))
    }
    "support boolean options" in {
      val schema = AvroSchema[OptionBoolean]

      val record1 = new GenericData.Record(schema)
      record1.put("b", true)
      Decoder[OptionBoolean].decode(record1, schema, DefaultFieldMapper) shouldBe OptionBoolean(Some(true))

      val record2 = new GenericData.Record(schema)
      record2.put("b", null)
      Decoder[OptionBoolean].decode(record2, schema, DefaultFieldMapper) shouldBe OptionBoolean(None)
    }
    "if a field is missing, use default value" in {
      val schema = AvroSchema[OptionStringDefault]

      val record1 = new GenericData.Record(AvroSchema[SchemaWithoutExpectedField])

      Decoder[OptionStringDefault].decode(record1, schema, DefaultFieldMapper) shouldBe OptionStringDefault(Some("cupcat"))
    }
    "if an enum field is missing, use default value" in {
      val schema = AvroSchema[OptionEnumDefault]

      val record1 = new GenericData.Record(AvroSchema[SchemaWithoutExpectedField])
      Decoder[OptionEnumDefault].decode(record1, schema, DefaultFieldMapper) shouldBe OptionEnumDefault(Some(CuppersOptionEnum))
    }
    "decode a null field to None" in {
      val schema = AvroSchema[OptionEnumDefaultWithNone]

      val record1 = new GenericData.Record(schema)
      record1.put("s", null)
      record1.put("t", "cupcat")

      Decoder[OptionEnumDefaultWithNone].decode(record1, schema, DefaultFieldMapper) shouldBe OptionEnumDefaultWithNone(None, "cupcat")
    }

  }
}

