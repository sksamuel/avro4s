package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroNamespace, AvroSchemaV2}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AvroNamespaceTest extends AnyWordSpec with Matchers {

  "@AvroNamespace" should {
    "support namespace annotations on records" in {

      @AvroNamespace("com.yuval") case class AnnotatedNamespace(s: String)

      val schema = AvroSchemaV2[AnnotatedNamespace]
      schema.getNamespace shouldBe "com.yuval"
    }

    "support namespace annotations in nested records" in {

      @AvroNamespace("com.yuval") case class AnnotatedNamespace(s: String, internal: InternalAnnotated)
      @AvroNamespace("com.yuval.internal") case class InternalAnnotated(i: Int)

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/namespace.avsc"))
      val schema = AvroSchemaV2[AnnotatedNamespace]
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support namespace annotations on field" in {

      case class InternalAnnotated(i: Int)
      @AvroNamespace("com.yuval") case class AnnotatedNamespace(s: String, @AvroNamespace("com.yuval.internal") internal: InternalAnnotated)

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/namespace.avsc"))
      val schema = AvroSchemaV2[AnnotatedNamespace]
      schema.toString(true) shouldBe expected.toString(true)
    }

    "favour namespace annotations on field over record" in {

      @AvroNamespace("ignore")
      case class InternalAnnotated(i: Int)

      @AvroNamespace("com.yuval") case class AnnotatedNamespace(s: String, @AvroNamespace("com.yuval.internal") internal: InternalAnnotated)

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/namespace.avsc"))
      val schema = AvroSchemaV2[AnnotatedNamespace]
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support namespace annotations on case classes at field level" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/namespace_enum_field_level.json"))
      val schema = AvroSchemaV2[Teapot]
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support namespace annotations on case classes at class level" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/namespace_enum_class_level.json"))
      val schema = AvroSchemaV2[Location]
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support namespace annotations on ADTs at type level" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/namespace_enum_trait_level.json"))
      val schema = AvroSchemaV2[Africa]
      schema.toString(true) shouldBe expected.toString(true)
    }

    "empty namespace" in {

      @AvroNamespace("")
      case class Foo(s: String)

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/namespace_empty.json"))
      val schema = AvroSchemaV2[Foo]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

sealed trait Tea
case object EarlGrey extends Tea
case object Assam extends Tea
case object EnglishBreakfast extends Tea

@AvroNamespace("wibble")
case class Teapot(@AvroNamespace("wobble") tea: Tea)

@AvroNamespace("wibble")
case class Location(africa: Africa)

@AvroNamespace("wobble")
sealed trait Africa
case object Cameroon extends Africa
case object Comoros extends Africa
case object Chad extends Africa
