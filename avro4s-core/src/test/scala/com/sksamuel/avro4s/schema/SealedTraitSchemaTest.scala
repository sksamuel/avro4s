package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema, AvroName}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SealedTraitSchemaTest extends AnyFunSuite with Matchers {

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

  test("trait of case objects at the top level should be encoded as enum") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/sealed_trait_of_objects.json"))
    val schema = AvroSchema[Dibble]
    schema.toString(true) shouldBe expected.toString(true)
  }

  sealed trait Foo extends Product with Serializable

  case object Bar extends Foo
  case object Baz extends Foo

  case class Schema(foo: Foo)

  test("trait of case objects at a nested level should be encoded as enum") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/sealed_trait_of_nested_objects.json"))
    val schema = AvroSchema[Schema]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("classes nested in objects should be encoded with consistent package name") {
    sealed trait Inner
    case class InnerOne(value: Double) extends Inner
    case class InnerTwo(height: Double) extends Inner
    case class Outer(inner: Inner)
    val schema = AvroSchema[Outer]
    schema.getNamespace shouldBe "com.sksamuel.avro4s.schema.SealedTraitSchemaTest"
  }

}

sealed trait Dibble
case object Dabble extends Dibble
@AvroName("Dobble")
case object Dobbles extends Dibble

sealed trait Wibble
case class Wobble(str: String) extends Wibble
case class Wabble(dbl: Double) extends Wibble
case class Wrapper(wibble: Wibble)

sealed trait Tibble
case class Tabble(str: Double, age: Int) extends Tibble
case class Tobble(str: String, place: String) extends Tibble
case class Trapper(tibble: Tibble)

sealed trait Nibble
case class Nabble(str: String, age: Int) extends Nibble
case class Nobble(str: String, place: String) extends Nibble
case class Napper(nibble: Nibble)
