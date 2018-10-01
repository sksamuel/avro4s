package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.{FunSuite, Matchers}

class SealedTraitSchemaTest extends FunSuite with Matchers {

  test("support sealed traits of case classes") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/sealed_traits.json"))
    val schema = AvroSchema[Wrapper]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support trait subtypes fields with same name") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/trait_subtypes_duplicate_fields.json"))
    val schema = AvroSchema[Trapper]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support trait subtypes fields with same name and same type") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/trait_subtypes_duplicate_fields_same_type.json"))
    val schema = AvroSchema[Napper]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support top level ADTs") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_sealed_trait.json"))
    val schema = AvroSchema[Nibble]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("trait of case objects should be encoded as enum") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/sealed_trait_of_objects.json"))
    val schema = AvroSchema[Dibble]
    schema.toString(true) shouldBe expected.toString(true)
  }
}

sealed trait Dibble
case object Dobble extends Dibble
case object Dabble extends Dibble

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
