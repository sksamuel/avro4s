package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchemaV2
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PrimitiveSchemaTest extends AnyWordSpec with Matchers {
  "SchemaEncoder" should {
    "support top level Booleans" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_boolean.json"))
      val schema = AvroSchemaV2[Boolean]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Longs" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_long.json"))
      val schema = AvroSchemaV2[Long]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Integers" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_integer.json"))
      val schema = AvroSchemaV2[Int]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Strings" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_string.json"))
      val schema = AvroSchemaV2[String]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Floats" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_float.json"))
      val schema = AvroSchemaV2[Float]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Doubles" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_double.json"))
      val schema = AvroSchemaV2[Double]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}
