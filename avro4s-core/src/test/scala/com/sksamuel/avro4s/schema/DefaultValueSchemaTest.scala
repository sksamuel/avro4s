package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchemaV2
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

case class Foo(gg: String = "wibble")

class DefaultValueSchemaTest extends AnyWordSpec with Matchers {

  "SchemaEncoder" should {
    "support default values for strings in top level classes" in {
      val schema = AvroSchemaV2[ClassWithDefaultString]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_string.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values for ints in top level classes" in {
      val schema = AvroSchemaV2[ClassWithDefaultInt]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_int.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values for booleans in top level classes" in {
      val schema = AvroSchemaV2[ClassWithDefaultBoolean]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_boolean.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values for doubles in top level classes" in {
      val schema = AvroSchemaV2[ClassWithDefaultDouble]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_double.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values for longs in top level classes" in {
      val schema = AvroSchemaV2[ClassWithDefaultLong]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_long.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values for floats in top level classes" in {
      val schema = AvroSchemaV2[ClassWithDefaultFloat]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_float.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values for maps and seqs" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/defaultvalues.json"))
      val schema = AvroSchemaV2[DefaultValues]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values set to None for optional sealed trait hierarchies" in {
      val schema = AvroSchemaV2[DogProspect]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_optional_union.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support default values of optional Seq and Map" in {
      val schema = AvroSchemaV2[OptionalDefaultValues]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optional_default_values.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support default values that are case classes" in {
      val schema = AvroSchemaV2[Cuppers]

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/case_class_default_values.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support default values that are case objects" in {
      implicit val schema = AvroSchemaV2[NoVarg]

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/case_object_default_values.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

sealed trait Dog
case class UnderDog(how_unfortunate: Double) extends Dog
case class UpperDog(how_fortunate: Double) extends Dog
case class DogProspect(dog: Option[Dog] = None)

case class OptionalDefaultValues(name: Option[String] = Some("sammy"),
                         age: Option[Int] = Some(21),
                         isFemale: Option[Boolean] = Some(false),
                         length: Option[Double] = Some(6.2),
                         timestamp: Option[Long] = Some(1468920998000l),
                         address: Option[Map[String, String]] = Some(Map(
                           "home" -> "sammy's home address",
                           "work" -> "sammy's work address"
                         )),
                         traits: Option[Seq[String]] = Some(Seq("Adventurous", "Helpful")),
                         favoriteWine: Option[Wine] = Some(Wine.CabSav))


case class ClassWithDefaultString(s: String = "foo")
case class ClassWithDefaultInt(i: Int = 123)
case class ClassWithDefaultBoolean(b: Boolean = true)
case class ClassWithDefaultLong(l: Long = 1468920998000l)
case class ClassWithDefaultFloat(f: Float = 123.458F)
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

sealed trait Cupcat
case object Rendal extends Cupcat
case class Snoutley(snoutley: String) extends Cupcat

case class Cuppers(cupcat: Cupcat = Snoutley("hates varg"))
case class NoVarg(cupcat: Cupcat = Rendal)
