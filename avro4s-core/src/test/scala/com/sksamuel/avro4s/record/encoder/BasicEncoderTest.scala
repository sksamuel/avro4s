package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, SchemaFor}
import com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage
import com.sksamuel.avro4s.{Encoder, ImmutableRecord}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.{Matchers, WordSpec}

class BasicEncoderTest extends WordSpec with Matchers {

  "Encoder" should {
    "encode strings as UTF8" in {
      case class Foo(s: String)
      val schema = AvroSchema[Foo]
      Encoder[Foo].encode(Foo("hello"), schema) shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
    }
    "encode strings as GenericData.Fixed when schema is fixed" in {
      case class Foo(s: String)
      implicit object StringFixedSchemaFor extends SchemaFor[String] {
        override def schema: Schema = Schema.createFixed("FixedString", null, null, 123)
      }
      val schema = AvroSchema[Foo]
      val record = Encoder[Foo].encode(Foo("hello"), schema).asInstanceOf[GenericRecord]
      record.get("s").asInstanceOf[GenericData.Fixed].bytes().toList shouldBe Seq(104, 101, 108, 108, 111)
    }
    "encode longs" in {
      case class Foo(l: Long)
      val schema = AvroSchema[Foo]
      Encoder[Foo].encode(Foo(123456L), schema) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(123456L)))
    }
    "encode doubles" in {
      case class Foo(d: Double)
      val schema = AvroSchema[Foo]
      Encoder[Foo].encode(Foo(123.435), schema) shouldBe ImmutableRecord(schema, Vector(java.lang.Double.valueOf(123.435D)))
    }
    "encode booleans" in {
      case class Foo(d: Boolean)
      val schema = AvroSchema[Foo]
      Encoder[Foo].encode(Foo(true), schema) shouldBe ImmutableRecord(schema, Vector(java.lang.Boolean.valueOf(true)))
    }
    "encode floats" in {
      case class Foo(d: Float)
      val schema = AvroSchema[Foo]
      Encoder[Foo].encode(Foo(123.435F), schema) shouldBe ImmutableRecord(schema, Vector(java.lang.Float.valueOf(123.435F)))
    }
    "encode ints" in {
      case class Foo(i: Int)
      val schema = AvroSchema[Foo]
      Encoder[Foo].encode(Foo(123), schema) shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(123)))
    }
    "support uppercase packages" in {
      val schema = AvroSchema[ClassInUppercasePackage]
      val encoder = Encoder[ClassInUppercasePackage]
      val t = com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage("hello")
      schema.getFullName shouldBe "com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage"
      encoder.encode(t, schema) shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
    }
  }
}


