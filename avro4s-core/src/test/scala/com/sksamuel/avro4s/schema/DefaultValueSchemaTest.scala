package com.sksamuel.avro4s.schema

import java.time.Instant

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

case class Foo(gg: String = "wibble")

class DefaultValueSchemaTest extends AnyWordSpec with Matchers {

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
    "support default values for instants in top level classes" in {
      implicit val schema = AvroSchema[ClassWithDefaultInstant]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_instant.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values for maps, sets and seqs" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/defaultvalues.json"))
      val schema = AvroSchema[DefaultValues]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values set to None for optional sealed trait hierarchies" in {
      val schema = AvroSchema[DogProspect]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_optional_union.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support default values of optional Seq, Set and Map" in {
      val schema = AvroSchema[OptionalDefaultValues]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optional_default_values.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support default values that are case classes" in {
      val schema = AvroSchema[Cuppers]

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/case_class_default_values.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support default values that are case objects" in {
      implicit val schema = AvroSchema[NoVarg]

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
                                 favoriteWine: Option[Wine] = Some(Wine.CabSav),
                                 luckyNumbers: Option[Set[Int]] = Some(Set(7, 9)),
                                 favoriteSongs: Option[Set[Song]] = Some(Set.empty[Song])
                                )


case class ClassWithDefaultString(s: String = "foo")
case class ClassWithDefaultInt(i: Int = 123)
case class ClassWithDefaultBoolean(b: Boolean = true)
case class ClassWithDefaultLong(l: Long = 1468920998000l)
case class ClassWithDefaultFloat(f: Float = 123.458F)
case class ClassWithDefaultDouble(d: Double = 123.456)
case class ClassWithDefaultInstant(min: Instant = Instant.MIN, max: Instant = Instant.MAX, epoch: Instant = Instant.EPOCH)

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
                         favoriteWine: Wine = Wine.CabSav,
                         luckyNumbers: Set[Int] = Set(7, 9),
                         favoriteSongs: Set[Song] = Set.empty[Song]
                        )

sealed trait Cupcat
case object Rendal extends Cupcat
case class Snoutley(snoutley: String) extends Cupcat

case class Cuppers(cupcat: Cupcat = Snoutley("hates varg"))
case class NoVarg(cupcat: Cupcat = Rendal)

case class Song(title: String)

