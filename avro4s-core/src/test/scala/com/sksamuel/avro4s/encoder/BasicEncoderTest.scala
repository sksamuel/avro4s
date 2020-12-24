package com.sksamuel.avro4s.encoder

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericFixed, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BasicEncoderTest extends AnyWordSpec with Matchers {

  "Encoder" should {
    "encode strings as UTF8" in {
      case class Foo(s: String)
      val schema = AvroSchema[Foo]
      val record = Encoder.derive[Foo].encode(schema)(Foo("hello")).asInstanceOf[GenericRecord]
      record.getSchema shouldBe schema
      record.get("s") shouldBe new Utf8("hello")
    }
//    "encode strings as GenericFixed and pad bytes when schema is fixed" in {
//      case class Foo(s: String)
//
//      val fixedSchema = SchemaFor[String](Schema.createFixed("FixedString", null, null, 7))
//      implicit val fixedStringEncoder: Encoder[String] = Encoder.StringEncoder.withSchema(fixedSchema)
//
//      val record = Encoder[Foo].encode(Foo("hello")).asInstanceOf[GenericRecord]
//      record.get("s").asInstanceOf[GenericFixed].bytes().toList shouldBe Seq(104, 101, 108, 108, 111, 0, 0)
//      // the fixed should have the right size
//      record.get("s").asInstanceOf[GenericFixed].bytes().length shouldBe 7
//    }
//    "encode longs" in {
//      case class Foo(l: Long)
//      val schema = AvroSchema[Foo]
//      Encoder[Foo].encode(Foo(123456L)) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(123456L)))
//    }
//    "encode doubles" in {
//      case class Foo(d: Double)
//      val schema = AvroSchema[Foo]
//      Encoder[Foo].encode(Foo(123.435)) shouldBe ImmutableRecord(schema, Vector(java.lang.Double.valueOf(123.435D)))
//    }
//    "encode booleans" in {
//      case class Foo(d: Boolean)
//      val schema = AvroSchema[Foo]
//      Encoder[Foo].encode(Foo(true)) shouldBe ImmutableRecord(schema, Vector(java.lang.Boolean.valueOf(true)))
//    }
//    "encode floats" in {
//      case class Foo(d: Float)
//      val schema = AvroSchema[Foo]
//      Encoder[Foo].encode(Foo(123.435F)) shouldBe ImmutableRecord(schema, Vector(java.lang.Float.valueOf(123.435F)))
//    }
//    "encode ints" in {
//      case class Foo(i: Int)
//      val schema = AvroSchema[Foo]
//      Encoder[Foo].encode(Foo(123)) shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(123)))
//    }
//    "support uppercase packages" in {
//      val schema = AvroSchema[ClassInUppercasePackage]
//      val t = com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage("hello")
//      schema.getFullName shouldBe "com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage"
//      Encoder[ClassInUppercasePackage].encode(t) shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
//    }
  }
}

//
//import com.sksamuel.avro4s.AvroValue
//import com.sksamuel.avro4s.{AvroSchema, Encoder, ImmutableRecord}
//import org.apache.avro.util.Utf8
//import org.junit.Test
//
//class BasicEncoderTest {
//
//  @Test def `encode strings as UTF8`() = {
//    case class Foo(s: String)
//    val schema = AvroSchema[Foo]
//    val encoder = Encoder.derived[Foo]
//    val value = encoder.encode(Foo("hello"), schema)
//    val expected = ImmutableRecord(schema, IndexedSeq(new Utf8("hello")))
//    value match {
//      case AvroValue.AvroRecord(record) =>
//        assert(record == expected)
//    }
//  }
//
//  @Test def `encode doubles`() = {
//    case class Foo(d: Double)
//    val schema = AvroSchema[Foo]
//    val encoder = Encoder.derived[Foo]
//    val value = encoder.encode(Foo(123.45), schema)
//    val expected = ImmutableRecord(schema, IndexedSeq(123.45))
//    value match {
//      case AvroValue.AvroRecord(record) =>
//        assert(record == expected)
//    }
//  }
//}
