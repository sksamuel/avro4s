package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, DefaultFieldMapper, Encoder, ImmutableRecord}
import com.sksamuel.avro4s.schema.{Colours, CupcatEnum, SnoutleyEnum, Wine}
import org.apache.avro.generic.GenericData.EnumSymbol
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EnumEncoderTest extends AnyWordSpec with Matchers {

  "Encoder" should {
    "encode java enums" in {
      case class Test(wine: Wine)
      val schema = AvroSchema[Test]
      val expected = ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("wine").schema(), "Malbec")))
      val actual = Encoder[Test].encode(Test(Wine.Malbec), schema, DefaultFieldMapper)
      actual shouldBe expected
    }
    "support optional java enums" in {
      case class Test(wine: Option[Wine])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(Some(Wine.Malbec)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("wine").schema(), "Malbec")))
      Encoder[Test].encode(Test(None), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(null))
    }
    "support scala enums" in {
      case class Test(value: Colours.Value)
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(Colours.Amber), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("value").schema(), "Amber")))
    }
    "support optional scala enums" in {
      case class Test(value: Option[Colours.Value])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(Some(Colours.Green)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("value").schema(), "Green")))
    }
    "support scala enums with defaults" in {
      case class Test(value: Colours.Value = Colours.Red)
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("value").schema(), "Red")))
    }
    "support sealed trait enums with defaults" in {
      case class Test(value: CupcatEnum = SnoutleyEnum)
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("value").schema(), "SnoutleyEnum")))
    }
  }
}

