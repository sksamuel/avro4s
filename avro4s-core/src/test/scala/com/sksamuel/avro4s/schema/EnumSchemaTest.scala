package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.SchemaEncoder
import org.scalatest.{Matchers, WordSpec}

class EnumSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "accept java enums" in {
      case class Test(wine: Wine)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/java_enum.json"))
      val schema = SchemaEncoder[Test].encode()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default options of enum values" in {
      val schema = SchemaEncoder[JavaEnumOptional].encode()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optional_java_enum.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support scala enums" in {
      val schema = SchemaEncoder[ScalaEnums].encode()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/scalaenums.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support option of scala enum values" in {
      val schema = SchemaEncoder[ScalaOptionEnums].encode()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optionscalaenum.json"))
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