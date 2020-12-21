package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroDoc, AvroSchema, FieldMapper, PascalCase, SchemaConfiguration, SnakeCase}
import org.apache.avro.Schema
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FieldMapperTest extends AnyWordSpec with Matchers {

  "AvroSchema" should {
    "use DefaultFieldMapper if no other is provided" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/field_mapper/default.json"))
      val schema = AvroSchema[NamingStrategyTest]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support pascal case" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/field_mapper/pascal.json"))
      val schema = AvroSchema[NamingStrategyTest](SchemaConfiguration.default.withMapper(PascalCase))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support snake case" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/field_mapper/snake.json"))
      val schema = AvroSchema[NamingStrategyTest](SchemaConfiguration.default.withMapper(SnakeCase))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class NamingStrategyTest(camelCase: String,
                              lower: String,
                              multipleWordsInThis: String,
                              StartsWithUpper: String,
                              nested: NamingStrategy2)

case class NamingStrategy2(camelCase: String)