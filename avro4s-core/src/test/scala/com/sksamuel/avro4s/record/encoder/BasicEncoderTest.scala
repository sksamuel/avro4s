package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{Encoder, ImmutableRecord, AvroSchema}
import org.scalatest.{Matchers, WordSpec}

class BasicEncoderTest extends WordSpec with Matchers {

  "Encoder" should {
    "encode strings" in {
      case class Foo(s: String)
      val schema = AvroSchema[Foo]
      Encoder[Foo].encode(Foo("hello"), schema) shouldBe ImmutableRecord(schema, Vector("hello"))
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
  }
}


