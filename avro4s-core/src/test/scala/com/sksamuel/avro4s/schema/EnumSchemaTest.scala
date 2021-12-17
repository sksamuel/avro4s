package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroEnumDefault, AvroName, AvroNamespace, AvroProp, AvroSchema, AvroSortPriority, AvroUnionPosition, JavaEnumSchemaFor, ScalaEnumSchemaFor, SchemaFor}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EnumSchemaTest extends AnyWordSpec with Matchers {

  implicit val schemaForWine: SchemaFor[Wine] = JavaEnumSchemaFor[Wine](default = Wine.Shiraz)
  implicit val schemaForColor: SchemaFor[Colours.Value] = ScalaEnumSchemaFor[Colours.Value](default = Colours.Amber)

  "SchemaEncoder" should {

    //------------------------------------------------------------------------------------------------------------------
    // java enums using the AvroJavaEnumDefault annotation

    "support top level java enums using the AvroJavaName, AvroJavaNamespace, AvroJavaProp, AvroJavaEnumDefault annotations" in {

      val schema = AvroSchema[WineWithAnnotations]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type": "enum",
          |  "name": "Wine",
          |  "namespace": "test",
          |  "symbols": [
          |    "Malbec",
          |    "Shiraz",
          |    "CabSav",
          |    "Merlot"
          |  ],
          |  "default": "CabSav",
          |  "hello": "world"
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    //------------------
    // java enums

    "support top level java enums" in {

      val schema = AvroSchema[Wine]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type": "enum",
          |  "name": "Wine",
          |  "namespace": "com.sksamuel.avro4s.schema",
          |  "symbols": [
          |    "Malbec",
          |    "Shiraz",
          |    "CabSav",
          |    "Merlot"
          |  ],
          |  "default": "Shiraz"
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support java enums" in {

      case class JavaEnum(wine: Wine)

      val schema = AvroSchema[JavaEnum]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type": "record",
          |  "name": "JavaEnum",
          |  "namespace" : "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields": [
          |    {
          |      "name": "wine",
          |      "type": {
          |        "type": "enum",
          |        "name": "Wine",
          |        "namespace" : "com.sksamuel.avro4s.schema",
          |        "symbols": [
          |          "Malbec",
          |          "Shiraz",
          |          "CabSav",
          |          "Merlot"
          |        ],
          |        "default": "Shiraz"
          |      }
          |    }
          |  ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support java enums with default values" in {

      case class JavaEnumWithDefaultValue(wine: Wine = Wine.CabSav)

      val schema = AvroSchema[JavaEnumWithDefaultValue]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type" : "record",
          |  "name" : "JavaEnumWithDefaultValue",
          |  "namespace" : "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields" : [ {
          |    "name" : "wine",
          |    "type" : {
          |      "type" : "enum",
          |      "name" : "Wine",
          |      "namespace" : "com.sksamuel.avro4s.schema",
          |      "symbols" : [ "Malbec", "Shiraz", "CabSav", "Merlot" ],
          |      "default" : "Shiraz"
          |    },
          |    "default" : "CabSav"
          |  } ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support optional java enums" in {

      case class OptionalJavaEnum(wine: Option[Wine])

      val schema = AvroSchema[OptionalJavaEnum]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type": "record",
          |  "name": "OptionalJavaEnum",
          |  "namespace": "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields": [
          |    {
          |      "name": "wine",
          |      "type": [
          |        "null",
          |        {
          |          "type": "enum",
          |          "name": "Wine",
          |          "namespace": "com.sksamuel.avro4s.schema",
          |          "symbols": [
          |            "Malbec",
          |            "Shiraz",
          |            "CabSav",
          |            "Merlot"
          |          ],
          |          "default": "Shiraz"
          |        }
          |      ]
          |    }
          |  ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support optional java enums with default none" in {

      case class OptionalJavaEnumWithDefaultNone(wine: Option[Wine] = None)

      val schema = AvroSchema[OptionalJavaEnumWithDefaultNone]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type": "record",
          |  "name": "OptionalJavaEnumWithDefaultNone",
          |  "namespace": "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields": [
          |    {
          |      "name": "wine",
          |      "type": [
          |        "null",
          |        {
          |          "type": "enum",
          |          "name": "Wine",
          |          "namespace": "com.sksamuel.avro4s.schema",
          |          "symbols": [
          |            "Malbec",
          |            "Shiraz",
          |            "CabSav",
          |            "Merlot"
          |          ],
          |          "default": "Shiraz"
          |        }
          |      ],
          |      "default": null
          |    }
          |  ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support optional java enums with default values" in {

      case class OptionalJavaEnumWithDefaultValue(wine: Option[Wine] = Some(Wine.CabSav))

      val schema = AvroSchema[OptionalJavaEnumWithDefaultValue]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type": "record",
          |  "name": "OptionalJavaEnumWithDefaultValue",
          |  "namespace": "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields": [
          |    {
          |      "name": "wine",
          |      "type": [
          |        {
          |          "type": "enum",
          |          "name": "Wine",
          |          "namespace": "com.sksamuel.avro4s.schema",
          |          "symbols": [
          |            "Malbec",
          |            "Shiraz",
          |            "CabSav",
          |            "Merlot"
          |          ],
          |          "default": "Shiraz"
          |        },
          |        "null"
          |      ],
          |      "default": "CabSav"
          |    }
          |  ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    //----------------------------------------
    // scala enums using ScalaEnumSchemaFor

    "support top level scala enums" in {

      val schema = AvroSchema[Colours.Value]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type": "enum",
          |  "name": "Colours",
          |  "namespace": "com.sksamuel.avro4s.schema",
          |  "symbols": [
          |    "Red",
          |    "Amber",
          |    "Green"
          |  ],
          |  "default": "Amber"
          |}
          |""".stripMargin.trim
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support scala enums" in {

      case class ScalaEnum(colours: Colours.Value)

      val schema = AvroSchema[ScalaEnum]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type" : "record",
          |  "name" : "ScalaEnum",
          |  "namespace" : "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields" : [ {
          |    "name" : "colours",
          |    "type" : {
          |      "type" : "enum",
          |      "name" : "Colours",
          |      "namespace" : "com.sksamuel.avro4s.schema",
          |      "symbols" : [ "Red", "Amber", "Green" ],
          |      "default": "Amber"
          |    }
          |  } ]
          |}
          |""".stripMargin.trim
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support scala enums with default values" in {

      case class ScalaEnumWithDefaultValue(colours: Colours.Value = Colours.Red)

      val schema = AvroSchema[ScalaEnumWithDefaultValue]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type": "record",
          |  "name": "ScalaEnumWithDefaultValue",
          |  "namespace": "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields": [
          |    {
          |      "name": "colours",
          |      "type": {
          |        "type": "enum",
          |        "name": "Colours",
          |        "namespace": "com.sksamuel.avro4s.schema",
          |        "symbols": [
          |          "Red",
          |          "Amber",
          |          "Green"
          |        ],
          |        "default": "Amber"
          |      },
          |      "default": "Red"
          |    }
          |  ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support optional scala enums" in {

      case class OptionalScalaEnum(color: Option[Colours.Value])

      val schema = AvroSchema[OptionalScalaEnum]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type": "record",
          |  "name": "OptionalScalaEnum",
          |  "namespace": "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields": [
          |    {
          |      "name": "color",
          |      "type": [
          |        "null",
          |        {
          |          "type": "enum",
          |          "namespace": "com.sksamuel.avro4s.schema",
          |          "name": "Colours",
          |          "symbols": [
          |            "Red",
          |            "Amber",
          |            "Green"
          |          ],
          |          "default": "Amber"
          |        }
          |      ]
          |    }
          |  ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support optional scala enums with default none" in {

      case class OptionalScalaEnumWithDefaultNone(color: Option[Colours.Value] = None)

      val schema = AvroSchema[OptionalScalaEnumWithDefaultNone]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type": "record",
          |  "name": "OptionalScalaEnumWithDefaultNone",
          |  "namespace": "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields": [
          |    {
          |      "name": "color",
          |      "type": [
          |        "null",
          |        {
          |          "type": "enum",
          |          "namespace": "com.sksamuel.avro4s.schema",
          |          "name": "Colours",
          |          "symbols": [
          |            "Red",
          |            "Amber",
          |            "Green"
          |          ],
          |          "default": "Amber"
          |        }
          |      ],
          |      "default": null
          |    }
          |  ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support optional scala enums with a default value" in {

      case class OptionalScalaEnumWithDefaultValue(coloursopt: Option[Colours.Value] = Option(Colours.Red))

      val schema = AvroSchema[OptionalScalaEnumWithDefaultValue]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type": "record",
          |  "name": "OptionalScalaEnumWithDefaultValue",
          |  "namespace": "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields": [
          |    {
          |      "name": "coloursopt",
          |      "type": [
          |        {
          |          "type": "enum",
          |          "namespace": "com.sksamuel.avro4s.schema",
          |          "name": "Colours",
          |          "symbols": [
          |            "Red",
          |            "Amber",
          |            "Green"
          |          ],
          |          "default": "Amber"
          |        },
          |        "null"
          |      ],
          |      "default": "Red"
          |    }
          |  ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    //------------------------------------------------------------------------------------------------------------------
    // scala enums using the AvroEnumDefault annotation

    "support top level scala enums using the AvroEnumDefault annotation" in {

      val schema = AvroSchema[ColoursAnnotatedEnum.Value]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type": "enum",
          |  "name": "Colours",
          |  "namespace": "test",
          |  "symbols": [
          |    "Red",
          |    "Amber",
          |    "Green"
          |  ],
          |  "default": "Green",
          |  "hello": "world"
          |}
          |""".stripMargin.trim
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    //------------------------------------------------------------------------------------------------------------------
    // sealed trait enums

    "support top level sealed trait enums with no default enum value" in {
      val schema = AvroSchema[CupcatEnum]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type" : "enum",
          |  "name" : "CupcatEnum",
          |  "namespace" : "com.sksamuel.avro4s.schema",
          |  "symbols" : [ "CuppersEnum", "SnoutleyEnum" ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support sealed trait enums with no default enum value" in {

      case class SealedTraitEnum(cupcat: CupcatEnum)

      val schema = AvroSchema[SealedTraitEnum]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type" : "record",
          |  "name" : "SealedTraitEnum",
          |  "namespace" : "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields" : [ {
          |    "name" : "cupcat",
          |    "type" : {
          |      "type" : "enum",
          |      "name" : "CupcatEnum",
          |      "namespace" : "com.sksamuel.avro4s.schema",
          |      "symbols" : [ "CuppersEnum", "SnoutleyEnum" ]
          |    }
          |  } ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support sealed trait enums with no default enum value and with a default field value" in {

      case class SealedTraitEnumWithDefaultValue(cupcat: CupcatEnum = CuppersEnum)

      val schema = AvroSchema[SealedTraitEnumWithDefaultValue]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type" : "record",
          |  "name" : "SealedTraitEnumWithDefaultValue",
          |  "namespace" : "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields" : [ {
          |    "name" : "cupcat",
          |    "type" : {
          |      "type" : "enum",
          |      "name" : "CupcatEnum",
          |      "namespace" : "com.sksamuel.avro4s.schema",
          |      "symbols" : [ "CuppersEnum", "SnoutleyEnum" ]
          |    },
          |    "default" : "CuppersEnum"
          |  } ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support optional sealed trait enums with no default enum value" in {

      case class OptionalSealedTraitEnum(cupcat: Option[CupcatEnum])

      val schema = AvroSchema[OptionalSealedTraitEnum]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type" : "record",
          |  "name" : "OptionalSealedTraitEnum",
          |  "namespace" : "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields" : [ {
          |    "name" : "cupcat",
          |    "type" : [ "null", {
          |      "type" : "enum",
          |      "name" : "CupcatEnum",
          |      "namespace" : "com.sksamuel.avro4s.schema",
          |      "symbols" : [ "CuppersEnum", "SnoutleyEnum" ]
          |    } ]
          |  } ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support optional sealed trait enums with no default enum value but with a default field value of none" in {

      case class OptionalSealedTraitEnumWithDefaultNone(cupcat: Option[CupcatEnum] = None)

      val schema = AvroSchema[OptionalSealedTraitEnumWithDefaultNone]

      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type" : "record",
          |  "name" : "OptionalSealedTraitEnumWithDefaultNone",
          |  "namespace" : "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields" : [ {
          |    "name" : "cupcat",
          |    "type" : [ "null", {
          |      "type" : "enum",
          |      "name" : "CupcatEnum",
          |      "namespace" : "com.sksamuel.avro4s.schema",
          |      "symbols" : [ "CuppersEnum", "SnoutleyEnum" ]
          |    } ],
          |    "default" : null
          |  } ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support optional sealed trait enums with no default enum value but with a default field value" in {

      case class OptionalSealedTraitEnumWithDefaultValue(cupcat: Option[CupcatEnum] = Option(SnoutleyEnum))

      val schema = AvroSchema[OptionalSealedTraitEnumWithDefaultValue]

      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type" : "record",
          |  "name" : "OptionalSealedTraitEnumWithDefaultValue",
          |  "namespace" : "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields" : [ {
          |    "name" : "cupcat",
          |    "type" : [ {
          |      "type" : "enum",
          |      "name" : "CupcatEnum",
          |      "namespace" : "com.sksamuel.avro4s.schema",
          |      "symbols" : [ "CuppersEnum", "SnoutleyEnum" ]
          |    }, "null" ],
          |    "default": "SnoutleyEnum"
          |  } ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    //------------------
    // sealed trait enums with annotation default

    "support sealed trait enums with a default enum value and no default field value" in {

      case class AnnotatedSealedTraitEnum(cupcat: CupcatAnnotatedEnum)

      val schema = AvroSchema[AnnotatedSealedTraitEnum]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type" : "record",
          |  "name" : "AnnotatedSealedTraitEnum",
          |  "namespace" : "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields" : [ {
          |    "name" : "cupcat",
          |    "type" : {
          |      "type" : "enum",
          |      "name" : "CupcatAnnotatedEnum",
          |      "namespace" : "com.sksamuel.avro4s.schema",
          |      "symbols" : [ "CuppersAnnotatedEnum", "SnoutleyAnnotatedEnum" ],
          |      "default" : "SnoutleyAnnotatedEnum"
          |    }
          |  } ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support sealed trait enums with a default enum value and a default field value" in {

      case class AnnotatedSealedTraitEnumWithDefaultValue(cupcat: CupcatAnnotatedEnum = CuppersAnnotatedEnum)

      val schema = AvroSchema[AnnotatedSealedTraitEnumWithDefaultValue]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type" : "record",
          |  "name" : "AnnotatedSealedTraitEnumWithDefaultValue",
          |  "namespace" : "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields" : [ {
          |    "name" : "cupcat",
          |    "type" : {
          |      "type" : "enum",
          |      "name" : "CupcatAnnotatedEnum",
          |      "namespace" : "com.sksamuel.avro4s.schema",
          |      "symbols" : [ "CuppersAnnotatedEnum", "SnoutleyAnnotatedEnum" ],
          |      "default" : "SnoutleyAnnotatedEnum"
          |    },
          |    "default" : "CuppersAnnotatedEnum"
          |  } ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support optional sealed trait enums with a default enum value and no default field value" in {

      case class OptionalAnnotatedSealedTraitEnum(cupcat: Option[CupcatAnnotatedEnum])

      val schema = AvroSchema[OptionalAnnotatedSealedTraitEnum]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type" : "record",
          |  "name" : "OptionalAnnotatedSealedTraitEnum",
          |  "namespace" : "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields" : [ {
          |    "name" : "cupcat",
          |    "type" : [ "null", {
          |      "type" : "enum",
          |      "name" : "CupcatAnnotatedEnum",
          |      "namespace" : "com.sksamuel.avro4s.schema",
          |      "symbols" : [ "CuppersAnnotatedEnum", "SnoutleyAnnotatedEnum" ],
          |      "default" : "SnoutleyAnnotatedEnum"
          |    } ]
          |  } ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support optional sealed trait enums with a default enum value and a default field value of none" in {

      case class OptionalAnnotatedSealedTraitEnumWithDefaultNone(cupcat: Option[CupcatAnnotatedEnum] = None)

      val schema = AvroSchema[OptionalAnnotatedSealedTraitEnumWithDefaultNone]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type" : "record",
          |  "name" : "OptionalAnnotatedSealedTraitEnumWithDefaultNone",
          |  "namespace" : "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields" : [ {
          |    "name" : "cupcat",
          |    "type" : [ "null", {
          |      "type" : "enum",
          |      "name" : "CupcatAnnotatedEnum",
          |      "namespace" : "com.sksamuel.avro4s.schema",
          |      "symbols" : [ "CuppersAnnotatedEnum", "SnoutleyAnnotatedEnum" ],
          |      "default" : "SnoutleyAnnotatedEnum"
          |    } ],
          |    "default": null
          |  } ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "support optional sealed trait enums with a default enum value and a default field value" in {

      case class OptionalAnnotatedSealedTraitEnumWithDefaultValue(cupcat: Option[CupcatAnnotatedEnum] = Option(CuppersAnnotatedEnum))

      val schema = AvroSchema[OptionalAnnotatedSealedTraitEnumWithDefaultValue]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
          |{
          |  "type" : "record",
          |  "name" : "OptionalAnnotatedSealedTraitEnumWithDefaultValue",
          |  "namespace" : "com.sksamuel.avro4s.schema.EnumSchemaTest",
          |  "fields" : [ {
          |    "name" : "cupcat",
          |    "type" : [ {
          |      "type" : "enum",
          |      "name" : "CupcatAnnotatedEnum",
          |      "namespace" : "com.sksamuel.avro4s.schema",
          |      "symbols" : [ "CuppersAnnotatedEnum", "SnoutleyAnnotatedEnum" ],
          |      "default" : "SnoutleyAnnotatedEnum"
          |    },
          |    "null" ],
          |    "default": "CuppersAnnotatedEnum"
          |  } ]
          |}
          |""".stripMargin
      )

      schema.toString(true) shouldBe expected.toString(true)
    }

    "changing enum namespace should preserve defaults" in {
      val schema = AvroSchema[Bowl]
      val expected = new org.apache.avro.Schema.Parser().parse(
        """
      {
        "type": "record",
        "name": "Bowl",
        "namespace": "com.fruit",
        "fields": [
        {
          "name": "fruit",
          "type": {
            "type": "enum",
            "name": "Fruit",
            "namespace": "com.fruit.bowl",
            "symbols": [
              "Unknown",
              "Orange",
              "Mango"
            ],
            "default": "Unknown"
          },
          "default": "Mango"
        }
        ]
      }
      """
      )
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

object Colours extends Enumeration {
  val Red, Amber, Green = Value
}

@AvroName("Colours")
@AvroNamespace("test")
@AvroEnumDefault(ColoursAnnotatedEnum.Green)
@AvroProp("hello", "world")
object ColoursAnnotatedEnum extends Enumeration {
  val Red, Amber, Green = Value
}

sealed trait CupcatEnum
@AvroSortPriority(0) case object SnoutleyEnum extends CupcatEnum
@AvroSortPriority(1) case object CuppersEnum extends CupcatEnum

@AvroEnumDefault(SnoutleyAnnotatedEnum)
sealed trait CupcatAnnotatedEnum
@AvroSortPriority(0) case object SnoutleyAnnotatedEnum extends CupcatAnnotatedEnum
@AvroSortPriority(1) case object CuppersAnnotatedEnum extends CupcatAnnotatedEnum

@AvroEnumDefault(Unknown)
@AvroNamespace("com.default.package")
sealed trait Fruit

@AvroUnionPosition(0) case object Unknown extends Fruit
@AvroUnionPosition(1) case object Orange  extends Fruit
@AvroUnionPosition(2) case object Mango   extends Fruit

@AvroNamespace("com.fruit")
case class Bowl(@AvroNamespace("com.fruit.bowl") fruit: Fruit = Mango)