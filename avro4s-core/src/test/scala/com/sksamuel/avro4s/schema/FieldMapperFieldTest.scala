package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema, FieldMapper, PascalCase, SnakeCase}
import org.scalatest.{Matchers, WordSpec}

class FieldMapperFieldTest extends WordSpec with Matchers {

  "fieldMapper" should {
    "defaultNoChange" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/field_mapper_default.json"))
      val schema = AvroSchema[NamingStrategyTest]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support pascal case" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/field_mapper_pascal.json"))
      implicit val pascal: FieldMapper = PascalCase
      val schema = AvroSchema[NamingStrategyTest]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support snake case" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/field_mapper_snake.json"))
      implicit val snake: FieldMapper = SnakeCase
      val schema = AvroSchema[NamingStrategyTest]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class NamingStrategyTest(camelCase: String, lower: String, multipleWordsInThis: String, StartsWithUpper: String, nested: NamingStrategy2)
case class NamingStrategy2(camelCase: String)