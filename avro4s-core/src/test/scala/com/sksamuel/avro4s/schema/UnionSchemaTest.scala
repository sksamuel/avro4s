package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.SchemaEncoder
import org.scalatest.{Matchers, WordSpec}
import shapeless.{:+:, CNil}

class UnionSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "support sealed traits of case classes" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/sealed_traits.json"))
      val schema = SchemaEncoder[Wrapper].encode()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support trait subtypes fields with same name" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/trait_subtypes_duplicate_fields.json"))
      val schema = SchemaEncoder[Trapper].encode()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support trait subtypes fields with same name and same type" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/trait_subtypes_duplicate_fields_same_type.json"))
      val schema = SchemaEncoder[Napper].encode()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support coproducts of coproducts" in {
      val single = SchemaEncoder[Union].encode()
      val unionOfUnions = SchemaEncoder[UnionOfUnions].encode()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/union.avsc"))
      single.toString(true) shouldBe expected.toString(true)
      unionOfUnions.toString(true) shouldBe expected.toString(true).replace("Union", "UnionOfUnions")
    }
  }
}

case class UnionOfUnions(union: (Int :+: String :+: CNil) :+: Boolean :+: CNil)

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
