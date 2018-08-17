package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.Wine
import com.sksamuel.avro4s.internal.SchemaEncoder
import org.scalatest.{Matchers, WordSpec}

case class Foo(gg: String = "wibble")

class DefaultValueSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "support default values for strings" in {
      val schema = SchemaEncoder[Foo].encode
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_string.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values for ints" in {
      case class Foo(bb: Int = 22)
      val schema = SchemaEncoder[Foo].encode
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_int.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }

    //    "support default values for booleans" in {
    //      case class Foo(b: Boolean = true)
    //      val schema = SchemaEncoder[Foo].encode
    //      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_boolean.json"))
    //      schema.toString(true) shouldBe expected.toString(true)
    //    }
    //    "support default values for doubles" in {
    //      case class Foo(d: Double = 123.456)
    //      val schema = SchemaEncoder[Foo].encode
    //      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_double.json"))
    //      schema.toString(true) shouldBe expected.toString(true)
    //    }
    //    "support default values for longs" in {
    //      case class Foo(a: Long = 123456789L)
    //      val schema = SchemaEncoder[Foo].encode
    //      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/default_values_long.json"))
    //      schema.toString(true) shouldBe expected.toString(true)
    //    }
    //    "support default option values" in {
    //      //val schema = SchemaFor[OptionDefaultValues]()
    //      //val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optiondefaultvalues.avsc"))
    //      //schema.toString(true) shouldBe expected.toString(true)
    //    }
  }
}

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

case class OptionDefaultValues(name: String = "sammy",
                               description: Option[String] = None,
                               currency: Option[String] = Some("$"))