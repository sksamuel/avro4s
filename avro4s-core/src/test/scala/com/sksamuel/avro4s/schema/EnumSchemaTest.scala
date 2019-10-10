package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroEnumDefault, AvroSchema, AvroSortPriority, ToRecord}
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
    "support top level enum schemas" in {
      val schema = AvroSchema[Wine]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_java_enum.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support default scala enum" in {
      val schema = AvroSchema[ScalaEnumsWithDefault]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_scala_enum.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support a default scala enum with sealed trait" in {
      val schema = AvroSchema[EnumsWithSealedTraitDefault]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_sealed_trait_enum.json"))

      schema.toString(true) shouldBe expected.toString(true)
    }

    "handle enum default in an option" in {
      val schema = AvroSchema[CupcatOptionalEnumDefault]

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_optional_enum.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }

  }
}

case class JavaEnumOptional(maybewine: Option[Wine])
case class JavaEnumOptionalWithDefault(maybewine: Option[Wine] = Some(Wine.CabSav))

object Colours extends Enumeration {
  val Red, Amber, Green = Value
}
case class ScalaEnums(colours: Colours.Value)
case class ScalaOptionEnums(coloursopt: Option[Colours.Value])

case class ScalaEnumsWithDefault(colours: Colours.Value = Colours.Red)

sealed trait CupcatEnum
@AvroSortPriority(0) case object SnoutleyEnum extends CupcatEnum
@AvroSortPriority(1) case object CuppersEnum extends CupcatEnum
case class EnumsWithSealedTraitDefault(cupcat: CupcatEnum = CuppersEnum)

@AvroEnumDefault(CuppersAnnotatedEnum)
sealed trait CupcatAnnotatedEnum
@AvroSortPriority(0) case object SnoutleyAnnotatedEnum extends CupcatAnnotatedEnum
@AvroSortPriority(1) case object CuppersAnnotatedEnum extends CupcatAnnotatedEnum

case object NotCupcat

sealed trait AnotherCupcatEnum
@AvroSortPriority(0) case object AnotherCuppersEnum extends AnotherCupcatEnum
@AvroSortPriority(1) case object AnotherSnoutleyEnum extends AnotherCupcatEnum

case class CupcatOptionalEnumDefault(cupcat: Option[AnotherCupcatEnum] = Option(AnotherSnoutleyEnum))

