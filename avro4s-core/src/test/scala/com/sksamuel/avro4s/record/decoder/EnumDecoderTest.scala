package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.AvroSchema
import com.sksamuel.avro4s.Decoder
import com.sksamuel.avro4s.schema.{Colours, Wine}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.EnumSymbol
import org.scalatest.{Matchers, WordSpec}

case class JavaEnumClass(wine: Wine)
case class JavaOptionEnumClass(wine: Option[Wine])

case class ScalaEnumClass(colour: Colours.Value)
case class ScalaOptionEnumClass(colour: Option[Colours.Value])

class EnumDecoderTest extends WordSpec with Matchers {

  "Decoder" should {
    "support java enums" in {
      val schema = AvroSchema[JavaEnumClass]
      val record = new GenericData.Record(schema)
      record.put("wine", new EnumSymbol(schema.getField("wine").schema(), "CabSav"))
      Decoder[JavaEnumClass].decode(record, schema) shouldBe JavaEnumClass(Wine.CabSav)
    }
    "support optional java enums" in {
      val schema = AvroSchema[JavaOptionEnumClass]
      val wineSchema = AvroSchema[Wine]

      val record1 = new GenericData.Record(schema)
      record1.put("wine", new EnumSymbol(wineSchema, "Merlot"))
      Decoder[JavaOptionEnumClass].decode(record1, schema) shouldBe JavaOptionEnumClass(Some(Wine.Merlot))

      val record2 = new GenericData.Record(schema)
      record2.put("wine", null)
      Decoder[JavaOptionEnumClass].decode(record2, schema) shouldBe JavaOptionEnumClass(None)
    }
    "support scala enums" in {
      val schema = AvroSchema[ScalaEnumClass]
      val record = new GenericData.Record(schema)
      record.put("colour", new EnumSymbol(schema.getField("colour").schema(), "Green"))
      Decoder[ScalaEnumClass].decode(record, schema) shouldBe ScalaEnumClass(Colours.Green)
    }
    "support optional scala enums" in {
      val schema = AvroSchema[ScalaOptionEnumClass]
      val colourSchema = AvroSchema[Colours.Value]

      val record1 = new GenericData.Record(schema)
      record1.put("colour", new EnumSymbol(colourSchema, "Amber"))
      Decoder[ScalaOptionEnumClass].decode(record1, schema) shouldBe ScalaOptionEnumClass(Some(Colours.Amber))

      val record2 = new GenericData.Record(schema)
      record2.put("colour", null)
      Decoder[ScalaOptionEnumClass].decode(record2, schema) shouldBe ScalaOptionEnumClass(None)
    }
  }
}