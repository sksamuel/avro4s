package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, DefaultNamingStrategy, Encoder, ImmutableRecord, NamingStrategy, SchemaFor}
import org.apache.avro.util.Utf8
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{FunSuite, Matchers}

class EncoderTypeclassOverrideTest extends FunSuite with Matchers {

  test("allow overriding built in Encoder implicit for a basic type") {

    implicit val StringAsBooleanSchemaFor = new SchemaFor[String] {
      override def schema(namingStrategy: NamingStrategy) = SchemaBuilder.builder().booleanType()
    }

    implicit val StringAsBooleanEncoder = new Encoder[String] {
      override def encode(t: String, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): AnyRef = java.lang.Boolean.valueOf(true)
    }

    case class OverrideTest(s: String, i: Int)

    val schema = AvroSchema[OverrideTest]
    val actual = Encoder[OverrideTest].encode(OverrideTest("hello", 123), schema)
    val expected = ImmutableRecord(schema, Vector(java.lang.Boolean.valueOf(true), java.lang.Integer.valueOf(123)))
    actual shouldBe expected
  }

  test("allow overriding built in Encoder implicit for a complex type") {

    implicit val FooOverrideSchemaFor = new SchemaFor[Foo] {
      override def schema(namingStrategy: NamingStrategy) = SchemaBuilder.builder().stringType()
    }

    implicit val FooOverrideEncoder = new Encoder[Foo] {
      override def encode(t: Foo, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): AnyRef = t.b.toString + ":" + t.i
    }

    case class Foo(b: Boolean, i: Int)
    case class OverrideTest(s: String, f: Foo)

    val schema = AvroSchema[OverrideTest]
    val actual = Encoder[OverrideTest].encode(OverrideTest("hello", Foo(true, 123)), schema)
    val expected = ImmutableRecord(schema, Vector(new Utf8("hello"), "true:123"))
    actual shouldBe expected
  }

  test("allow overriding built in Encoder implicit for a value type") {

    implicit object FooValueTypeSchemaFor extends SchemaFor[FooValueType] {
      override def schema(namingStrategy: NamingStrategy) = SchemaBuilder.builder().intType()
    }

    implicit object FooValueTypeEncoder extends Encoder[FooValueType] {
      override def encode(t: FooValueType, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): AnyRef = java.lang.Integer.valueOf(t.s.toInt)
    }

    case class OverrideTest(s: String, foo: FooValueType)

    val schema = AvroSchema[OverrideTest]
    val actual = Encoder[OverrideTest].encode(OverrideTest("hello", FooValueType("123")), schema)
    val expected = ImmutableRecord(schema, Vector(new Utf8("hello"), java.lang.Integer.valueOf(123)))
    actual shouldBe expected
  }

  test("allow overriding built in Encoder implicit for a top level value type") {

    implicit object FooValueTypeSchemaFor extends SchemaFor[FooValueType] {
      override def schema(namingStrategy: NamingStrategy) = SchemaBuilder.builder().intType()
    }

    implicit object FooValueTypeEncoder extends Encoder[FooValueType] {
      override def encode(t: FooValueType, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): AnyRef = java.lang.Integer.valueOf(t.s.toInt)
    }

    val schema = AvroSchema[FooValueType]
    Encoder[FooValueType].encode(FooValueType("123"), schema) shouldBe java.lang.Integer.valueOf(123)
    Encoder[FooValueType].encode(FooValueType("5455"), schema) shouldBe java.lang.Integer.valueOf(5455)
  }
}
