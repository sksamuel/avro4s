package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.schema._
import com.sksamuel.avro4s.{AvroEnumDefault, AvroSchema, AvroSchemaV2, Decoder, DecoderV2, DefaultFieldMapper, SchemaForV2}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.EnumSymbol
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

case class JavaEnumClass(wine: Wine)
case class JavaOptionEnumClass(wine: Option[Wine])

case class ScalaEnumClass(colour: Colours.Value)
case class ScalaOptionEnumClass(colour: Option[Colours.Value])
case class ScalaEnumClassWithDefault(colour: Colours.Value = Colours.Red)
case class ScalaSealedTraitEnumWithDefault(cupcat: CupcatEnum = SnoutleyEnum)
case class ScalaAnnotatedSealedTraitEnumWithDefault(cupcat: CupcatAnnotatedEnum = CuppersAnnotatedEnum)
case class ScalaAnnotatedSealedTraitEnumList(@AvroEnumDefault(List(CuppersAnnotatedEnum)) cupcat: List[CupcatAnnotatedEnum])


class EnumDecoderTest extends AnyWordSpec with Matchers {

  "Decoder" should {
    "support java enums" in {
      val schema = AvroSchemaV2[JavaEnumClass]
      val record = new GenericData.Record(schema)
      record.put("wine", new EnumSymbol(schema.getField("wine").schema(), "CabSav"))
      DecoderV2[JavaEnumClass].decode(record) shouldBe JavaEnumClass(Wine.CabSav)
    }
    "support optional java enums" in {
      val schema = AvroSchemaV2[JavaOptionEnumClass]
      val wineSchema = AvroSchemaV2[Wine]

      val record1 = new GenericData.Record(schema)
      record1.put("wine", new EnumSymbol(wineSchema, "Merlot"))
      DecoderV2[JavaOptionEnumClass].decode(record1) shouldBe JavaOptionEnumClass(Some(Wine.Merlot))

      val record2 = new GenericData.Record(schema)
      record2.put("wine", null)
      DecoderV2[JavaOptionEnumClass].decode(record2) shouldBe JavaOptionEnumClass(None)
    }
    "support scala enums" in {
      val schema = AvroSchemaV2[ScalaEnumClass]
      val record = new GenericData.Record(schema)
      record.put("colour", new EnumSymbol(schema.getField("colour").schema(), "Green"))
      DecoderV2[ScalaEnumClass].decode(record) shouldBe ScalaEnumClass(Colours.Green)
    }
    "support optional scala enums" in {
      val schema = AvroSchemaV2[ScalaOptionEnumClass]
      val colourSchema = AvroSchemaV2[Colours.Value]

      val record1 = new GenericData.Record(schema)
      record1.put("colour", new EnumSymbol(colourSchema, "Amber"))
      DecoderV2[ScalaOptionEnumClass].decode(record1) shouldBe ScalaOptionEnumClass(Some(Colours.Amber))

      val record2 = new GenericData.Record(schema)
      record2.put("colour", null)
      DecoderV2[ScalaOptionEnumClass].decode(record2) shouldBe ScalaOptionEnumClass(None)
    }
    "support scala enum default values" in {
      val schema = AvroSchemaV2[ScalaEnumClassWithDefault]
      val record = new GenericData.Record(schema)

      record.put("colour", new EnumSymbol(schema.getField("colour").schema(), "Puce"))
      DecoderV2[ScalaEnumClassWithDefault].decode(record) shouldBe ScalaEnumClassWithDefault(Colours.Red)
    }
    "support sealed trait enum default values in a record" in {
      val schema = AvroSchemaV2[ScalaSealedTraitEnumWithDefault]
      val record = new GenericData.Record(schema)

      record.put("cupcat", new EnumSymbol(schema.getField("cupcat").schema(), "NoVarg"))
      DecoderV2[ScalaSealedTraitEnumWithDefault].decode(record) shouldBe ScalaSealedTraitEnumWithDefault(SnoutleyEnum)
    }
    "support annotated sealed trait enum default values" ignore {
      val schema = AvroSchemaV2[CupcatAnnotatedEnum]
      val record = new EnumSymbol(schema, NotCupcat)

      // TODO is this really intended to work? it decodes an enum as case class with enum field.
      DecoderV2[ScalaAnnotatedSealedTraitEnumWithDefault].decode(record) shouldBe ScalaAnnotatedSealedTraitEnumWithDefault(CuppersAnnotatedEnum)
    }
    "support annotated sealed trait enum default values - corrected" in {
      val schema = AvroSchemaV2[CupcatAnnotatedEnum]
      val e = new EnumSymbol(schema, NotCupcat)
      val record = new GenericData.Record(AvroSchemaV2[ScalaAnnotatedSealedTraitEnumWithDefault])
      record.put("cupcat", e)
      DecoderV2[ScalaAnnotatedSealedTraitEnumWithDefault].decode(record) shouldBe ScalaAnnotatedSealedTraitEnumWithDefault(CuppersAnnotatedEnum)
    }
  }
}
