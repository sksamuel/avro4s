package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.AvroSchema
import org.scalatest.{Matchers, WordSpec}

class PrimitiveSchemaTest extends WordSpec with Matchers {
  "SchemaEncoder" should {
    "support top level Booleans" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_boolean.avsc"))
      val schema = AvroSchema[Boolean]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Longs" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_long.avsc"))
      val schema = AvroSchema[Long]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Integers" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_integer.avsc"))
      val schema = AvroSchema[Int]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Strings" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_string.avsc"))
      val schema = AvroSchema[String]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Floats" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_float.avsc"))
      val schema = AvroSchema[Float]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Doubles" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_double.avsc"))
      val schema = AvroSchema[Double]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}
