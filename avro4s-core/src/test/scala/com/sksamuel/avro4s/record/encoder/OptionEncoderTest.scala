package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, Encoder, ImmutableRecord, SchemaFor}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class OptionEncoderTest extends AnyWordSpec with Matchers {

  "Encoder" should {
    "support String options" in {
      case class Test(o: Option[String])
      val schema = AvroSchema[Test]
      val expectedSome = ImmutableRecord(schema, Vector(new Utf8("qwe")))
      val expectedNone = ImmutableRecord(schema, Vector(null))

      Encoder[Test].encode(schema)(Test(Some("qwe"))) shouldBe expectedSome
      Encoder[Test].encode(schema)(Test(None)) shouldBe expectedNone
    }

    "support boolean options" in {
      case class Test(o: Option[Boolean])
      val schema = AvroSchema[Test]
      val expectedSome = ImmutableRecord(schema, Vector(java.lang.Boolean.valueOf(true)))
      val expectedNone = ImmutableRecord(schema, Vector(null))

      Encoder[Test].encode(schema)(Test(Some(true))) shouldBe expectedSome
      Encoder[Test].encode(schema)(Test(None)) shouldBe expectedNone
    }

    "support options of case classes" in {
      case class Foo(s: String)
      case class Test(o: Option[Foo])
      val schema = AvroSchema[Test]
      val fooSchema = AvroSchema[Foo]
      val expectedSome = ImmutableRecord(schema, Vector(ImmutableRecord(fooSchema, Vector(new Utf8("hello")))))
      val expectedNone = ImmutableRecord(schema, Vector(null))

      Encoder[Test].encode(schema)(Test(Some(Foo("hello")))) shouldBe expectedSome
      Encoder[Test].encode(schema)(Test(None)) shouldBe expectedNone
    }

    "support options of either" in {
      case class Test(o: Option[Either[String, Boolean]])
      val schema = AvroSchema[Test]
      val expectedSomeLeft = ImmutableRecord(schema, Vector(new Utf8("hello")))
      val expectedSomeRight = ImmutableRecord(schema, Vector(java.lang.Boolean.valueOf(true)))
      val expectedNone = ImmutableRecord(schema, Vector(null))

      Encoder[Test].encode(schema)(Test(Some(Left("hello")))) shouldBe expectedSomeLeft
      Encoder[Test].encode(schema)(Test(Some(Right(true)))) shouldBe expectedSomeRight
      Encoder[Test].encode(schema)(Test(None)) shouldBe expectedNone
    }

    "support options of sealed traits" in {
      sealed trait T
      case class A(a: String) extends T
      case class B(b: String) extends T
      case class Test(o: Option[T])
      val schema = AvroSchema[Test]
      val aSchema = AvroSchema[A]
      val bSchema = AvroSchema[B]
      val expectedSomeA = ImmutableRecord(schema, Vector(ImmutableRecord(aSchema, Vector(new Utf8("hello")))))
      val expectedSomeB = ImmutableRecord(schema, Vector(ImmutableRecord(bSchema, Vector(new Utf8("hello")))))
      val expectedNone = ImmutableRecord(schema, Vector(null))

      Encoder[Test].encode(schema)(Test(Some(A("hello")))) shouldBe expectedSomeA
      Encoder[Test].encode(schema)(Test(Some(B("hello")))) shouldBe expectedSomeB
      Encoder[Test].encode(schema)(Test(None)) shouldBe expectedNone
    }

    "support options of enums" in {
      sealed trait T
      case object A extends T
      case object B extends T
      case class Test(o: Option[T])
      val schema = AvroSchema[Test]
      val tSchema = AvroSchema[T]
      val expectedSomeA = ImmutableRecord(schema, Vector(new GenericData.EnumSymbol(tSchema, A)))
      val expectedSomeB = ImmutableRecord(schema, Vector(new GenericData.EnumSymbol(tSchema, B)))
      val expectedNone = ImmutableRecord(schema, Vector(null))

      Encoder[Test].encode(schema)(Test(Some(A))) shouldBe expectedSomeA
      Encoder[Test].encode(schema)(Test(Some(B))) shouldBe expectedSomeB
      Encoder[Test].encode(schema)(Test(None)) shouldBe expectedNone
    }
  }
}
