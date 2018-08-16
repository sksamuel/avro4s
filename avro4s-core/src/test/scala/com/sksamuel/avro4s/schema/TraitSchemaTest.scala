package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{Napper, SchemaFor, Trapper, Wrapper}
import org.scalatest.{Matchers, WordSpec}

class TraitSchemaTest extends WordSpec with Matchers {
  "SchemaEncoder" should {
    "support sealed traits" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/sealed_traits.avsc"))
      val schema = SchemaFor[Wrapper]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support trait subtypes fields with same name" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/trait_subtypes_duplicate_fields.avsc"))
      val schema = SchemaFor[Trapper]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support trait subtypes fields with same name and same type" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/trait_subtypes_duplicate_fields_same_type.avsc"))
      val schema = SchemaFor[Napper]()
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}
