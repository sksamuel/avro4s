package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroEnumDefault, AvroSchema, AvroValue, Decoder}
import com.sksamuel.avro4s.schema.{Colours, CupcatAnnotatedEnum, CupcatEnum, CuppersAnnotatedEnum, SnoutleyEnum, Wine}
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
      val schema = AvroSchema[JavaEnumClass]
      val record = new GenericData.Record(schema)
      record.put("wine", new EnumSymbol(schema.getField("wine").schema(), "CabSav"))
      Decoder[JavaEnumClass].decode(AvroValue.unsafeFromAny(record)) shouldBe JavaEnumClass(Wine.CabSav)
    }
    "support optional java enums" in {
      val schema = AvroSchema[JavaOptionEnumClass]
      val wineSchema = AvroSchema[Wine]

      val record1 = new GenericData.Record(schema)
      record1.put("wine", new EnumSymbol(wineSchema, "Merlot"))
      Decoder[JavaOptionEnumClass].decode(AvroValue.unsafeFromAny(record1)) shouldBe JavaOptionEnumClass(Some(Wine.Merlot))

      val record2 = new GenericData.Record(schema)
      record2.put("wine", null)
      Decoder[JavaOptionEnumClass].decode(AvroValue.unsafeFromAny(record2)) shouldBe JavaOptionEnumClass(None)
    }
    "support scala enums" in {
      val schema = AvroSchema[ScalaEnumClass]
      val record = new GenericData.Record(schema)
      record.put("colour", new EnumSymbol(schema.getField("colour").schema(), "Green"))
      Decoder[ScalaEnumClass].decode(AvroValue.unsafeFromAny(record)) shouldBe ScalaEnumClass(Colours.Green)
    }
    "support optional scala enums" in {
      val schema = AvroSchema[ScalaOptionEnumClass]
      val colourSchema = AvroSchema[Colours.Value]

      val record1 = new GenericData.Record(schema)
      record1.put("colour", new EnumSymbol(colourSchema, "Amber"))
      Decoder[ScalaOptionEnumClass].decode(AvroValue.unsafeFromAny(record1)) shouldBe ScalaOptionEnumClass(Some(Colours.Amber))

      val record2 = new GenericData.Record(schema)
      record2.put("colour", null)
      Decoder[ScalaOptionEnumClass].decode(AvroValue.unsafeFromAny(record2)) shouldBe ScalaOptionEnumClass(None)
    }
    "support scala enum default values" in {
      val schema = AvroSchema[ScalaEnumClassWithDefault]
      val record = new GenericData.Record(schema)

      record.put("colour", new EnumSymbol(schema.getField("colour").schema(), "Puce"))
      Decoder[ScalaEnumClassWithDefault].decode(AvroValue.unsafeFromAny(record)) shouldBe ScalaEnumClassWithDefault(Colours.Red)
    }
    "support sealed trait enum default values in a record" in {
      val schema = AvroSchema[ScalaSealedTraitEnumWithDefault]
      val record = new GenericData.Record(schema)

      record.put("cupcat", new EnumSymbol(schema.getField("cupcat").schema(), "NoVarg"))
      Decoder[ScalaSealedTraitEnumWithDefault].decode(AvroValue.unsafeFromAny(record)) shouldBe ScalaSealedTraitEnumWithDefault(SnoutleyEnum)
    }
    "support annotated sealed trait enum default values" ignore {

      case object NotCupcat

      val schema = AvroSchema[CupcatAnnotatedEnum]
      val record = new EnumSymbol(schema, NotCupcat)

      // TODO clarify is this really intended to work? it decodes an enum as case class with enum field.
      Decoder[ScalaAnnotatedSealedTraitEnumWithDefault].decode(AvroValue.unsafeFromAny(record)) shouldBe ScalaAnnotatedSealedTraitEnumWithDefault(CuppersAnnotatedEnum)
    }

    "support annotated sealed trait enum default values - corrected" in {

      case object NotCupcat

      val schema = AvroSchema[CupcatAnnotatedEnum]
      val e = new EnumSymbol(schema, NotCupcat)
      val record = new GenericData.Record(AvroSchema[ScalaAnnotatedSealedTraitEnumWithDefault])
      record.put("cupcat", e)
      Decoder[ScalaAnnotatedSealedTraitEnumWithDefault].decode(AvroValue.unsafeFromAny(record)) shouldBe ScalaAnnotatedSealedTraitEnumWithDefault(CuppersAnnotatedEnum)
    }
  }
}
