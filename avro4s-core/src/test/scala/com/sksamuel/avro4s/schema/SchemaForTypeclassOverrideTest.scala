package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema, SchemaFor}
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SchemaForTypeclassOverrideTest extends AnyFunSuite with Matchers {

  test("allow overriding built in SchemaFor implicit for a basic type") {

    implicit val StringSchemaFor = SchemaFor[String] {
      val schema = SchemaBuilder.builder().bytesType()
      schema.addProp("foo", "bar": AnyRef)
      schema
    }

    case class OverrideTest(s: String, i: Int)

    val expected = new Schema.Parser().parse(getClass.getResourceAsStream("/schemas/override/basic.json"))
    val schema = AvroSchema[OverrideTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("allow overriding built in SchemaFor implicit for a complex type") {

    implicit val FooSchemaFor = SchemaFor[Foo] {
      val schema = SchemaBuilder.builder().doubleType()
      schema.addProp("foo", "bar": AnyRef)
      schema
    }

    case class Foo(s: String, b: Boolean)
    case class OverrideTest(s: String, f: Foo)

    val expected = new Schema.Parser().parse(getClass.getResourceAsStream("/schemas/override/complex.json"))
    val schema = AvroSchema[OverrideTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

//  test("allow overriding built in SchemaFor implicit for a value type") {
//
//    implicit val FooValueTypeSchemaFor = SchemaFor[FooValueType] {
//      val schema = SchemaBuilder.builder().intType()
//      schema.addProp("foo", "bar": AnyRef)
//      schema
//    }
//
//    case class OverrideTest(s: String, foo: FooValueType)
//
//    val expected =
//      new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schema_override_value_type.json"))
//    val schema = AvroSchema[FooValueType]
//    schema.toString(true) shouldBe expected.toString(true)
//  }
//
//  test("allow overriding built in SchemaFor implicit for a top level value type") {
//
//    implicit val FooValueTypeSchemaFor = SchemaFor[FooValueType] {
//      val schema = SchemaBuilder.builder().intType()
//      schema.addProp("foo", "bar": AnyRef)
//      schema
//    }
//
//    val expected = new org.apache.avro.Schema.Parser()
//      .parse(getClass.getResourceAsStream("/schema_override_top_level_value_type.json"))
//    val schema = AvroSchema[FooValueType]
//    schema.toString(true) shouldBe expected.toString(true)
//  }
}

case class FooValueType(s: String) extends AnyVal
