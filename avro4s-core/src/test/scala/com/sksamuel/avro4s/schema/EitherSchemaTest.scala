package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.scalatest.{Matchers, WordSpec}

class EitherSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "generate union:T,U for Either[T,U] of primitives" in {
      case class Test(either: Either[String, Double])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/either.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate union:T,U for Either[T,U] of records" in {
      case class Goo(s: String)
      case class Foo(b: Boolean)
      case class Test(either: Either[Goo, Foo])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/either_record.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate union:T,U for Either[T,U] of records using @AvroNamespace" in {
      @AvroNamespace("mm")
      case class Goo(s: String)
      @AvroNamespace("nn")
      case class Foo(b: Boolean)
      case class Test(either: Either[Goo, Foo])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/either_record_with_avro_namespace.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}
