package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.AvroSchema
import org.scalatest.{Matchers, WordSpec}

case class Foo(gg: String = "wibble")

class DefaultValueSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "support default values for strings in top level classes" in {
      val schema = AvroSchema[ClassWithDefaultString]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_string.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values for ints in top level classes" in {
      val schema = AvroSchema[ClassWithDefaultInt]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_int.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values for booleans in top level classes" in {
      val schema = AvroSchema[ClassWithDefaultBoolean]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_boolean.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values for doubles in top level classes" in {
      val schema = AvroSchema[ClassWithDefaultDouble]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_double.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values for longs in top level classes" in {
      val schema = AvroSchema[ClassWithDefaultLong]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_long.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values for floats in top level classes" in {
      val schema = AvroSchema[ClassWithDefaultFloat]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_float.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class ClassWithDefaultString(s: String = "foo")
case class ClassWithDefaultInt(i: Int = 123)
case class ClassWithDefaultBoolean(b: Boolean = true)
case class ClassWithDefaultLong(l: Long = 1468920998000l)
case class ClassWithDefaultFloat(f: Float = 123.456F)
case class ClassWithDefaultDouble(d: Double = 123.456)

case class DefaultValues(name: String = "sammy",
                         age: Int = 21,
                         isFemale: Boolean = false,
                         length: Double = 6.2,
                         timestamp: Long = 1468920998000l,
                         address: Map[String, String] = Map(
                           "home" -> "sammy's home address",
                           "work" -> "sammy's work address"
                         ),
                         traits: Seq[String] = Seq("Adventurous", "Helpful"),
                         favoriteWine: Wine = Wine.CabSav)