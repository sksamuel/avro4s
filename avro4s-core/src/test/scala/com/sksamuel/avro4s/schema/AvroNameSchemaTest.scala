package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroName, AvroSchema}
import org.scalatest.{FunSuite, Matchers}

class AvroNameSchemaTest extends FunSuite with Matchers {

  test("generate field names using @AvroName") {
    case class Foo(@AvroName("wibble") wobble: String, wubble: String)
    val schema = AvroSchema[Foo]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_name_field.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate class names using @AvroName") {
    @AvroName("wibble")
    case class Foo(a: String, b: String)
    val schema = AvroSchema[Foo]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_name_class.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }
}