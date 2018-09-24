package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{Encoder, InternalRecord, AvroSchema}
import com.sksamuel.avro4s.schema.{Colours, Wine}
import org.apache.avro.generic.GenericData.EnumSymbol
import org.scalatest.{Matchers, WordSpec}

class JavaEnumEncoderTest extends WordSpec with Matchers {

  "Encoder" should {
    "encode java enums" in {
      case class Test(wine: Wine)
      val schema = AvroSchema[Test]
      val expected = InternalRecord(schema, Vector(new EnumSymbol(schema.getField("wine").schema(), "Malbec")))
      val actual = Encoder[Test].encode(Test(Wine.Malbec), schema)
      actual shouldBe expected
    }
    "support optional java enums" in {
      case class Test(wine: Option[Wine])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(Some(Wine.Malbec)), schema) shouldBe InternalRecord(schema, Vector(new EnumSymbol(schema.getField("wine").schema(), "Malbec")))
      Encoder[Test].encode(Test(None), schema) shouldBe InternalRecord(schema, Vector(null))
    }
    "support scala enums" in {
      case class Test(value: Colours.Value)
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(Colours.Amber), schema) shouldBe InternalRecord(schema, Vector(new EnumSymbol(schema.getField("value").schema(), "Amber")))
    }
    "support optional scala enums" in {
      case class Test(value: Option[Colours.Value])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(Some(Colours.Green)), schema) shouldBe InternalRecord(schema, Vector(new EnumSymbol(schema.getField("value").schema(), "Green")))
    }
  }
}

