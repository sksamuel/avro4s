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

  test("@AvroName on top level java enum") {
    val schema = AvroSchema[MyJavaEnum]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_name_java_enum.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("@AvroName on field level java enum") {
    case class Wibble(e: MyJavaEnum)
    val schema = AvroSchema[Wibble]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_name_nested_java_enum.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }
}
