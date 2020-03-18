package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroNamespace, AvroSchemaV2}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EitherSchemaTest extends AnyWordSpec with Matchers {

  "SchemaEncoder" should {
    "generate union:T,U for Either[T,U] of primitives" in {
      case class Test(either: Either[String, Double])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/either.json"))
      val schema = AvroSchemaV2[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate union:T,U for Either[T,U] of records" in {
      case class Goo(s: String)
      case class Foo(b: Boolean)
      case class Test(either: Either[Goo, Foo])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/either_record.json"))
      val schema = AvroSchemaV2[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate union:T,U for Either[T,U] of records using @AvroNamespace" in {
      @AvroNamespace("mm")
      case class Goo(s: String)
      @AvroNamespace("nn")
      case class Foo(b: Boolean)
      case class Test(either: Either[Goo, Foo])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/either_record_with_avro_namespace.json"))
      val schema = AvroSchemaV2[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "flatten nested unions and move null to first position" in {
      AvroSchemaV2[Either[String, Option[Int]]].toString shouldBe """["null","string","int"]"""
    }
  }
}
