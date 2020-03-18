package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, Encoder, ImmutableRecord, SchemaFor}
import org.apache.avro.SchemaBuilder
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EncoderTypeclassOverrideTest extends AnyFunSuite with Matchers {

  test("allow overriding built in Encoder implicit for a basic type") {

    implicit val StringAsBooleanSchemaFor = SchemaFor[String](SchemaBuilder.builder().booleanType())

    implicit val StringAsBooleanEncoder = new Encoder[String] {
      val schemaFor: SchemaFor[String] = StringAsBooleanSchemaFor

      def encode(value: String): AnyRef = java.lang.Boolean.valueOf(true)
    }

    case class OverrideTest(s: String, i: Int)

    val schema = AvroSchema[OverrideTest]
    val actual = Encoder[OverrideTest].encode(OverrideTest("hello", 123))
    val expected = ImmutableRecord(schema, Vector(java.lang.Boolean.valueOf(true), java.lang.Integer.valueOf(123)))
    actual shouldBe expected
  }

  test("allow overriding built in Encoder implicit for a complex type") {

    implicit val FooOverrideSchemaFor = SchemaFor[Foo](SchemaBuilder.builder().stringType())

    implicit val FooOverrideEncoder = new Encoder[Foo] {

      val schemaFor: SchemaFor[Foo] = FooOverrideSchemaFor

      def encode(value: Foo): AnyRef = value.b.toString + ":" + value.i
    }

    case class Foo(b: Boolean, i: Int)
    case class OverrideTest(s: String, f: Foo)

    val schema = AvroSchema[OverrideTest]
    val actual = Encoder[OverrideTest].encode(OverrideTest("hello", Foo(true, 123)))
    val expected = ImmutableRecord(schema, Vector(new Utf8("hello"), "true:123"))
    actual shouldBe expected
  }

  test("allow overriding built in Encoder implicit for a value type") {

    implicit val FooValueTypeSchemaFor = SchemaFor[FooValueType](SchemaBuilder.builder().intType())


    implicit object FooValueTypeEncoder extends Encoder[FooValueType] {

      val schemaFor: SchemaFor[FooValueType] = FooValueTypeSchemaFor

      def encode(value: FooValueType): AnyRef = java.lang.Integer.valueOf(value.s.toInt)
    }

    case class OverrideTest(s: String, foo: FooValueType)

    val schema = AvroSchema[OverrideTest]
    val actual = Encoder[OverrideTest].encode(OverrideTest("hello", FooValueType("123")))
    val expected = ImmutableRecord(schema, Vector(new Utf8("hello"), java.lang.Integer.valueOf(123)))
    actual shouldBe expected
  }

  test("allow overriding built in Encoder implicit for a top level value type") {

    implicit val FooValueTypeSchemaFor = SchemaFor[FooValueType](SchemaBuilder.builder().intType())

    implicit object FooValueTypeEncoder extends Encoder[FooValueType] {

      def schemaFor = FooValueTypeSchemaFor

      def encode(value: FooValueType): AnyRef = java.lang.Integer.valueOf(value.s.toInt)
    }

    val schema = AvroSchema[FooValueType]
    Encoder[FooValueType].encode(FooValueType("123")) shouldBe java.lang.Integer.valueOf(123)
    Encoder[FooValueType].encode(FooValueType("5455")) shouldBe java.lang.Integer.valueOf(5455)
  }
}
