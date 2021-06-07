package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema, FieldMapper, PascalCase, SchemaFor, SnakeCase}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FieldMapperFieldTest extends AnyWordSpec with Matchers {

  "fieldMapper" should {
    "defaultNoChange" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/field_mapper_default.json"))
      val schema = SchemaFor[NamingStrategyTest].schema
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support pascal case" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/field_mapper_pascal.json"))
      val schema = SchemaFor[NamingStrategyTest].withFieldMapper(PascalCase).schema
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support snake case" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/field_mapper_snake.json"))
      val schema = SchemaFor[NamingStrategyTest].withFieldMapper(SnakeCase).schema
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class NamingStrategyTest(camelCase: String, lower: String, multipleWordsInThis: String, StartsWithUpper: String, nested: NamingStrategy2)
case class NamingStrategy2(camelCase: String)