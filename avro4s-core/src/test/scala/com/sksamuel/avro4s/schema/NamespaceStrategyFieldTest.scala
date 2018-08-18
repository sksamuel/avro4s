package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.SchemaEncoder
import com.sksamuel.avro4s.{PascalCase, SnakeCase}
import org.scalatest.{Matchers, WordSpec}

class NamespaceStrategyFieldTest extends WordSpec with Matchers {

  "NamingStrategy" should {
    "support pascal case" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/naming_strategy_pascal.json"))
      val schema = SchemaEncoder[NamingStrategyTest].encode(PascalCase)
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support snake case" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/naming_strategy_snake.json"))
      val schema = SchemaEncoder[NamingStrategyTest].encode(SnakeCase)
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class NamingStrategyTest(camelCase: String, lower: String, multipleWordsInThis: String, StartsWithUpper: String)