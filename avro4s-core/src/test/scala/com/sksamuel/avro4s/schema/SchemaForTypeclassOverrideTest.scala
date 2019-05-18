package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema, NamingStrategy, SchemaFor}
import org.apache.avro.SchemaBuilder
import org.scalatest.{FunSuite, Matchers}

class SchemaForTypeclassOverrideTest extends FunSuite with Matchers {

  test("allow overriding built in SchemaFor implicit for a basic type") {

    implicit val StringSchemaFor = new SchemaFor[String] {
      override def schema(implicit namingStrategy: NamingStrategy) = {
        val schema = SchemaBuilder.builder().bytesType()
        schema.addProp("foo", "bar": AnyRef)
        schema
      }
    }

    case class OverrideTest(s: String, i: Int)

    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schema_override_basic.json"))
    val schema = AvroSchema[OverrideTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("allow overriding built in SchemaFor implicit for a complex type") {

    implicit val FooSchemaFor = new SchemaFor[Foo] {
      override def schema(implicit namingStrategy: NamingStrategy) = {
        val schema = SchemaBuilder.builder().doubleType()
        schema.addProp("foo", "bar": AnyRef)
        schema
      }
    }

    case class Foo(s: String, b: Boolean)
    case class OverrideTest(s: String, f: Foo)

    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schema_override_complex.json"))
    val schema = AvroSchema[OverrideTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("allow overriding built in SchemaFor implicit for a value type") {

    implicit object FooValueTypeSchemaFor extends SchemaFor[FooValueType] {
      override def schema(implicit namingStrategy: NamingStrategy) = {
        val schema = SchemaBuilder.builder().intType()
        schema.addProp("foo", "bar": AnyRef)
        schema
      }
    }

    case class OverrideTest(s: String, foo: FooValueType)

    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schema_override_value_type.json"))
    val schema = AvroSchema[FooValueType]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("allow overriding built in SchemaFor implicit for a top level value type") {

    implicit object FooValueTypeSchemaFor extends SchemaFor[FooValueType] {
      override def schema(implicit namingStrategy: NamingStrategy) = {
        val schema = SchemaBuilder.builder().intType()
        schema.addProp("foo", "bar": AnyRef)
        schema
      }
    }

    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schema_override_top_level_value_type.json"))
    val schema = AvroSchema[FooValueType]
    schema.toString(true) shouldBe expected.toString(true)
  }
}

case class FooValueType(s: String) extends AnyVal