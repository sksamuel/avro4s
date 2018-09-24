package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.AvroSchema
import org.scalatest.{Matchers, WordSpec}

class EnumSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "accept java enums" in {
      case class Test(wine: Wine)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/java_enum.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support options of java enum values" in {
      val schema = AvroSchema[JavaEnumOptional]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optional_java_enum.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values in options of java enum values" in {
      val schema = AvroSchema[JavaEnumOptionalWithDefault]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optional_java_enum_with_default.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support scala enums" in {
      val schema = AvroSchema[ScalaEnums]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/scalaenums.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support option of scala enum values" in {
      val schema = AvroSchema[ScalaOptionEnums]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optional_scala_enum.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class JavaEnumOptional(maybewine: Option[Wine])
case class JavaEnumOptionalWithDefault(maybewine: Option[Wine] = Some(Wine.CabSav))

object Colours extends Enumeration {
  val Red, Amber, Green = Value
}

case class ScalaEnums(value: Colours.Value)

case class ScalaOptionEnums(value: Option[Colours.Value])