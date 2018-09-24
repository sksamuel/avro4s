package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.SchemaFor
import com.sksamuel.avro4s.{NamingStrategy, PascalCase, SnakeCase}
import org.scalatest.{Matchers, WordSpec}

class NamespaceStrategyFieldTest extends WordSpec with Matchers {

  "NamingStrategy" should {
    "support pascal case" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/naming_strategy_pascal.json"))
      implicit val pascal: NamingStrategy = PascalCase
      val schema = SchemaFor[NamingStrategyTest]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support snake case" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/naming_strategy_snake.json"))
      implicit val snake: NamingStrategy = SnakeCase
      val schema = SchemaFor[NamingStrategyTest]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class NamingStrategyTest(camelCase: String, lower: String, multipleWordsInThis: String, StartsWithUpper: String)