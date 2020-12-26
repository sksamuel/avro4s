package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s._
import com.sksamuel.avro4s.encoders.Encoder
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
      val record = Encoder[Foo].encode(schema)(Foo("hello")).asInstanceOf[GenericRecord]
      record.getSchema shouldBe schema
      record.get("s") shouldBe new Utf8("hello")
    }
    "encode longs" in {
      case class Foo(l: Long)
      val schema = AvroSchema[Foo]
      val record = Encoder[Foo].encode(schema)(Foo(123456L)).asInstanceOf[GenericRecord]
      record.getSchema shouldBe schema
      record.get("l") shouldBe java.lang.Long.valueOf(123456L)
    }
    "encode ints" in {
      case class Foo(i: Int)
      val schema = AvroSchema[Foo]
      val record = Encoder[Foo].encode(schema)(Foo(123)).asInstanceOf[GenericRecord]
      record.getSchema shouldBe schema
      record.get("i") shouldBe java.lang.Integer.valueOf(123)
    }
    "encode doubles" in {
      case class Foo(d: Double)
      val schema = AvroSchema[Foo]
      val record = Encoder[Foo].encode(schema)(Foo(123.435)).asInstanceOf[GenericRecord]
      record.getSchema shouldBe schema
      record.get("d") shouldBe java.lang.Double.valueOf(123.435D)
    }
    "encode booleans" in {
      case class Foo(b: Boolean)
      val schema = AvroSchema[Foo]
      val record = Encoder[Foo].encode(schema)(Foo(true)).asInstanceOf[GenericRecord]
      record.getSchema shouldBe schema
      record.get("b") shouldBe java.lang.Boolean.valueOf(true)
    }
    "encode floats" in {
      case class Foo(f: Float)
      val schema = AvroSchema[Foo]
      val record = Encoder[Foo].encode(schema)(Foo(123.435F)).asInstanceOf[GenericRecord]
      record.getSchema shouldBe schema
      record.get("f") shouldBe java.lang.Float.valueOf(123.435F)
    }
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
