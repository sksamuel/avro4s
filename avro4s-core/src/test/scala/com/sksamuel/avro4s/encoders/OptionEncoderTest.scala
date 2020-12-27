package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.encoders.Encoder
import com.sksamuel.avro4s.{AvroSchema, ImmutableRecord}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class OptionEncoderTest extends AnyWordSpec with Matchers {

  "Encoder" should {
    "support String options" in {
      case class Test(s: Option[String])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(schema)(Test(Option("qwe"))) match {
        case record: GenericRecord => record.get("s") shouldBe new Utf8("qwe")
      }
      Encoder[Test].encode(schema)(Test(None)) match {
        case record: GenericRecord => record.get("s") shouldBe null
      }
    }
    "support boolean options" in {
      case class Test(b: Option[Boolean])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(schema)(Test(Option(true))) match {
        case record: GenericRecord => record.get("b") shouldBe java.lang.Boolean.valueOf(true)
      }
      Encoder[Test].encode(schema)(Test(None)) match {
        case record: GenericRecord => record.get("b") shouldBe null
      }
    }
    "support double options" in {
      case class Test(x: Option[Double])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(schema)(Test(Option(213.3))) match {
        case record: GenericRecord => record.get("x") shouldBe java.lang.Double.valueOf(213.3)
      }
      Encoder[Test].encode(schema)(Test(None)) match {
        case record: GenericRecord => record.get("x") shouldBe null
      }
    }
    "support long options" in {
      case class Test(x: Option[Long])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(schema)(Test(Option(213L))) match {
        case record: GenericRecord => record.get("x") shouldBe java.lang.Long.valueOf(213)
      }
      Encoder[Test].encode(schema)(Test(None)) match {
        case record: GenericRecord => record.get("x") shouldBe null
      }
    }
//    "support options of case classes" in {
//      case class Foo(s: String)
//      case class Test(b: Option[Foo])
//      val schema = AvroSchema[Test]
//      val fooSchema = AvroSchema[Foo]
//      Encoder[Test].encode(schema)(Test(Option(Foo("hello")))) shouldBe ImmutableRecord(schema, Vector(ImmutableRecord(fooSchema, Vector(new Utf8("hello")))))
//      Encoder[Test].encode(schema)(Test(None)) shouldBe ImmutableRecord(schema, Vector(null))
//    }
//    "support schema overrides with either" in {
//      case class Test(a: Option[Either[String, Int]])
//      val expected = AvroSchema[Test]
//      val schema = Encoder[Test].withSchema(SchemaFor[Test]).schema
//      schema shouldBe expected
//    }
  }
}