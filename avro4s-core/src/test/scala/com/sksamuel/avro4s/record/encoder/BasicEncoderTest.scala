package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage
import com.sksamuel.avro4s.{AvroSchema, Encoder, ImmutableRecord, SchemaFor}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericFixed, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BasicEncoderTest extends AnyWordSpec with Matchers {

  "Encoder" should {
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
      val t = ClassInUppercasePackage("hello")
      schema.getFullName shouldBe "com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage"
      Encoder[ClassInUppercasePackage].encode(t, schema) shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
    }
  }
}
