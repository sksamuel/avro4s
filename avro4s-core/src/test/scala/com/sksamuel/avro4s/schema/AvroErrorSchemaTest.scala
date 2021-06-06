package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroError, AvroSchema}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AvroErrorSchemaTest extends AnyFunSuite with Matchers {

  test("@AvroError should set schema type to Error when used on a case class") {
    @AvroError case class Flibble(val a: String)
    val schema = AvroSchema[Flibble]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_error.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("@AvroError should set schema type to Error when used on a field") {
    case class VeryCross(@AvroError flibble: Flibble)
    case class Flibble(val a: String)
    val schema = AvroSchema[VeryCross]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_error_field.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }
}
