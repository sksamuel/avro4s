package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.SchemaFor
import org.scalatest.{Matchers, WordSpec}

class TraitSchemaTest extends WordSpec with Matchers {
  "SchemaEncoder" should {
    "support sealed traits" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/sealed_traits.avsc"))
      val schema = SchemaFor[Wrapper]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support trait subtypes fields with same name" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/trait_subtypes_duplicate_fields.avsc"))
      val schema = SchemaFor[Trapper]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support trait subtypes fields with same name and same type" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/trait_subtypes_duplicate_fields_same_type.avsc"))
      val schema = SchemaFor[Napper]()
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

sealed trait Wibble
case class Wobble(str: String) extends Wibble
case class Wabble(dbl: Double) extends Wibble
case class Wrapper(wibble: Wibble)

sealed trait Tibble
case class Tobble(str: String, place: String) extends Tibble
case class Tabble(str: Double, age: Int) extends Tibble
case class Trapper(tibble: Tibble)

sealed trait Nibble
case class Nobble(str: String, place: String) extends Nibble
case class Nabble(str: String, age: Int) extends Nibble
case class Napper(nibble: Nibble)
