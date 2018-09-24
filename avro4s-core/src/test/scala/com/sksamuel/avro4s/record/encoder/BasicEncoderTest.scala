package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{Encoder, InternalRecord, AvroSchema}
import org.scalatest.{Matchers, WordSpec}

class BasicEncoderTest extends WordSpec with Matchers {

  "Encoder" should {
    "encode strings" in {
      case class Foo(s: String)
      val schema = AvroSchema[Foo]
      Encoder[Foo].encode(Foo("hello"), schema) shouldBe InternalRecord(schema, Vector("hello"))
    }
    "encode longs" in {
      case class Foo(l: Long)
      val schema = AvroSchema[Foo]
      Encoder[Foo].encode(Foo(123456L), schema) shouldBe InternalRecord(schema, Vector(123456L))
    }
    "encode doubles" in {
      case class Foo(d: Double)
      val schema = AvroSchema[Foo]
      Encoder[Foo].encode(Foo(123.435), schema) shouldBe InternalRecord(schema, Vector(123.435D))
    }
    "encode booleans" in {
      case class Foo(d: Boolean)
      val schema = AvroSchema[Foo]
      Encoder[Foo].encode(Foo(true), schema) shouldBe InternalRecord(schema, Vector(true))
    }
    "encode floats" in {
      case class Foo(d: Float)
      val schema = AvroSchema[Foo]
      Encoder[Foo].encode(Foo(123.435F), schema) shouldBe InternalRecord(schema, Vector(123.435F))
    }
  }
}


