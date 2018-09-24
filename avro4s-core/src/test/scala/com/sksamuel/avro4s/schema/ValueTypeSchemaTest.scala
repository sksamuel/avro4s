package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.SchemaFor
import org.scalatest.{Matchers, WordSpec}

class ValueTypeSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "support value class at the top level" in {
      val schema = SchemaFor[ValueClass]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/value_class_top_level.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support value class as a nested field" in {
      case class Wibble(value: ValueClass)
      val schema = SchemaFor[Wibble]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/value_class_nested.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class ValueClass(str: String) extends AnyVal
