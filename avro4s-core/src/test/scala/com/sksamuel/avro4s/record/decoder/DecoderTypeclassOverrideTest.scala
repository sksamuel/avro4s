package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s._
import org.apache.avro.SchemaBuilder
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DecoderTypeclassOverrideTest extends AnyFunSuite with Matchers {

  case class StringOverrideTest(s: String, i: Int)

  case class Foo(b: Boolean, i: Int)
  case class FooOverrideTest(s: String, f: Foo)

  case class ValueTypeOverrideTest(s: String, f: FooValueType)

  test("allow overriding built in Decoder implicit for a basic type") {

    implicit val StringAsBooleanSchemaFor = SchemaForV2[String](SchemaBuilder.builder().booleanType())

    implicit val StringAsBooleanDecoder = new Decoder[String] {

      val schemaFor: SchemaForV2[String] = StringAsBooleanSchemaFor

      override def decode(value: Any): String = value match {
        case true => "a"
        case false => "b"
        case _ => sys.error("Only supporting booleans")
      }
    }

    val schema = AvroSchemaV2[StringOverrideTest]

    val record1 = ImmutableRecord(schema, Vector(java.lang.Boolean.valueOf(true), java.lang.Integer.valueOf(123)))
    Decoder[StringOverrideTest].decode(record1) shouldBe StringOverrideTest("a", 123)

    val record2 = ImmutableRecord(schema, Vector(java.lang.Boolean.valueOf(false), java.lang.Integer.valueOf(123)))
    Decoder[StringOverrideTest].decode(record2) shouldBe StringOverrideTest("b", 123)
  }

  test("allow overriding built in Decoder implicit for a complex type") {

    implicit val FooOverrideSchemaFor = SchemaForV2[Foo](SchemaBuilder.builder().stringType())

    implicit val FooOverrideEncoder = new Decoder[Foo] {

      val schemaFor: SchemaForV2[Foo] = FooOverrideSchemaFor

      override def decode(value: Any): Foo = value match {
        case string: String =>
          val tokens = string.split(':')
          Foo(tokens(0).toBoolean, tokens(1).toInt)
      }
    }

    val schema = AvroSchemaV2[FooOverrideTest]

    val record1 = ImmutableRecord(schema, Vector("a", "true:123"))
    Decoder[FooOverrideTest].decode(record1) shouldBe FooOverrideTest("a", Foo(true, 123))

    val record2 = ImmutableRecord(schema, Vector("b", "false:555"))
    Decoder[FooOverrideTest].decode(record2) shouldBe FooOverrideTest("b", Foo(false, 555))
  }

  test("allow overriding built in Decoder implicit for a value type") {

    implicit val FooValueTypeOverrideSchemaFor = SchemaForV2[FooValueType](SchemaBuilder.builder().intType())

    implicit val FooValueTypeOverrideDecoder = new Decoder[FooValueType] {

      val schemaFor: SchemaForV2[FooValueType] = FooValueTypeOverrideSchemaFor

      override def decode(value: Any): FooValueType = value match {
        case i: Int => FooValueType(i.toString)
        case i: java.lang.Integer => FooValueType(i.toString)
      }
    }

    val schema = AvroSchema[ValueTypeOverrideTest]

    val record1 = ImmutableRecord(schema, Vector("a", java.lang.Integer.valueOf(123)))
    Decoder[ValueTypeOverrideTest].decode(record1) shouldBe ValueTypeOverrideTest("a", FooValueType("123"))

    val record2 = ImmutableRecord(schema, Vector("b", java.lang.Integer.valueOf(555)))
    Decoder[ValueTypeOverrideTest].decode(record2) shouldBe ValueTypeOverrideTest("b", FooValueType("555"))
  }

  test("allow overriding built in Decoder implicit for a top level value type") {

    implicit val FooValueTypeOverrideSchemaFor = SchemaForV2[FooValueType](SchemaBuilder.builder().intType())

    implicit val FooValueTypeOverrideDecoder = new Decoder[FooValueType] {

      val schemaFor: SchemaForV2[FooValueType] = FooValueTypeOverrideSchemaFor

      override def decode(value: Any): FooValueType = value match {
        case i: Int => FooValueType(i.toString)
        case i: java.lang.Integer => FooValueType(i.toString)
      }
    }

    Decoder[FooValueType].decode(java.lang.Integer.valueOf(555)) shouldBe FooValueType("555")
    Decoder[FooValueType].decode(java.lang.Integer.valueOf(1234)) shouldBe FooValueType("1234")
  }
}
