package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage
import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericFixed, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BasicEncoderTest extends AnyWordSpec with Matchers {

  "Encoder" should {
    "encode strings as UTF8" in {
      case class Foo(s: String)
      val schema = AvroSchemaV2[Foo]
      val record = Encoder[Foo].encode(Foo("hello"))
      record shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
    }
    "encode strings as GenericFixed and pad bytes when schema is fixed" in {
      case class Foo(s: String)

      val fixedSchema = SchemaForV2[String](Schema.createFixed("FixedString", null, null, 7))
      implicit val fixedStringEncoder: Encoder[String] = Encoder.StringEncoder.withSchema(fixedSchema)

      val record = Encoder[Foo].encode(Foo("hello")).asInstanceOf[GenericRecord]
      record.get("s").asInstanceOf[GenericFixed].bytes().toList shouldBe Seq(104, 101, 108, 108, 111, 0, 0)
      // the fixed should have the right size
      record.get("s").asInstanceOf[GenericFixed].bytes().length shouldBe 7
    }
    "encode longs" in {
      case class Foo(l: Long)
      val schema = AvroSchemaV2[Foo]
      Encoder[Foo].encode(Foo(123456L)) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(123456L)))
    }
    "encode doubles" in {
      case class Foo(d: Double)
      val schema = AvroSchemaV2[Foo]
      Encoder[Foo].encode(Foo(123.435)) shouldBe ImmutableRecord(schema, Vector(java.lang.Double.valueOf(123.435D)))
    }
    "encode booleans" in {
      case class Foo(d: Boolean)
      val schema = AvroSchemaV2[Foo]
      Encoder[Foo].encode(Foo(true)) shouldBe ImmutableRecord(schema, Vector(java.lang.Boolean.valueOf(true)))
    }
    "encode floats" in {
      case class Foo(d: Float)
      val schema = AvroSchemaV2[Foo]
      Encoder[Foo].encode(Foo(123.435F)) shouldBe ImmutableRecord(schema, Vector(java.lang.Float.valueOf(123.435F)))
    }
    "encode ints" in {
      case class Foo(i: Int)
      val schema = AvroSchemaV2[Foo]
      Encoder[Foo].encode(Foo(123)) shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(123)))
    }
    "support uppercase packages" in {
      val schema = AvroSchemaV2[ClassInUppercasePackage]
      val t = com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage("hello")
      schema.getFullName shouldBe "com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage"
      Encoder[ClassInUppercasePackage].encode(t) shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
    }
  }
}
