package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{SchemaFor, Wine}
import org.scalatest.{Matchers, WordSpec}

class EnumSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "accept java enums" in {
      case class Test(wine: Wine)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/enum.json"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default options of enum values" in {
      val schema = SchemaFor[JavaEnumOptional]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optional_java_enum.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support scala enums" in {
      val schema = SchemaFor[ScalaEnums]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/scalaenums.avsc"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support option of scala enum values" in {
      val schema = SchemaFor[ScalaOptionEnums]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optionscalaenum.avsc"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class JavaEnumOptional(maybewine: Option[Wine])

object Colours extends Enumeration {
  val Red, Amber, Green = Value
}
case class ScalaEnums(value: Colours.Value)

case class ScalaOptionEnums(value: Option[Colours.Value])