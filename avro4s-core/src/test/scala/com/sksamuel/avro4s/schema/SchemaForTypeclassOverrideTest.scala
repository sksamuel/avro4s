package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchemaV2, SchemaForV2}
import org.apache.avro.SchemaBuilder
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SchemaForTypeclassOverrideTest extends AnyFunSuite with Matchers {

  test("allow overriding built in SchemaFor implicit for a basic type") {

    implicit val StringSchemaFor = SchemaForV2[String] {
      val schema = SchemaBuilder.builder().bytesType()
      schema.addProp("foo", "bar": AnyRef)
      schema
    }

    case class OverrideTest(s: String, i: Int)

    val expected =
      new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schema_override_basic.json"))
    val schema = AvroSchemaV2[OverrideTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("allow overriding built in SchemaFor implicit for a complex type") {

    implicit val FooSchemaFor = SchemaForV2[Foo] {
      val schema = SchemaBuilder.builder().doubleType()
      schema.addProp("foo", "bar": AnyRef)
      schema
    }

    case class Foo(s: String, b: Boolean)
    case class OverrideTest(s: String, f: Foo)

    val expected =
      new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schema_override_complex.json"))
    val schema = AvroSchemaV2[OverrideTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("allow overriding built in SchemaFor implicit for a value type") {

    implicit val FooValueTypeSchemaFor = SchemaForV2[FooValueType] {
      val schema = SchemaBuilder.builder().intType()
      schema.addProp("foo", "bar": AnyRef)
      schema
    }

    case class OverrideTest(s: String, foo: FooValueType)

    val expected =
      new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schema_override_value_type.json"))
    val schema = AvroSchemaV2[FooValueType]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("allow overriding built in SchemaFor implicit for a top level value type") {

    implicit val FooValueTypeSchemaFor = SchemaForV2[FooValueType] {
      val schema = SchemaBuilder.builder().intType()
      schema.addProp("foo", "bar": AnyRef)
      schema
    }

    val expected = new org.apache.avro.Schema.Parser()
      .parse(getClass.getResourceAsStream("/schema_override_top_level_value_type.json"))
    val schema = AvroSchemaV2[FooValueType]
    schema.toString(true) shouldBe expected.toString(true)
  }
}

case class FooValueType(s: String) extends AnyVal
