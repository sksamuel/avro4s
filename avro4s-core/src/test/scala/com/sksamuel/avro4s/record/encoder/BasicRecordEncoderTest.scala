package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{InternalRecord, RecordEncoder, SchemaEncoder}
import org.scalatest.{Matchers, WordSpec}

class BasicRecordEncoderTest extends WordSpec with Matchers {

  "RecordEncoder" should {
    "encode strings" in {
      case class Foo(s: String)
      val schema = SchemaEncoder[Foo].encode()
      RecordEncoder[Foo](schema).encode(Foo("hello")) shouldBe InternalRecord(schema, Vector("hello"))
    }
    "encode longs" in {
      case class Foo(l: Long)
      val schema = SchemaEncoder[Foo].encode()
      RecordEncoder[Foo](schema).encode(Foo(123456L)) shouldBe InternalRecord(schema, Vector(123456L))
    }
    "encode doubles" in {
      case class Foo(d: Double)
      val schema = SchemaEncoder[Foo].encode()
      RecordEncoder[Foo](schema).encode(Foo(123.435)) shouldBe InternalRecord(schema, Vector(123.435D))
    }
    "encode booleans" in {
      case class Foo(d: Boolean)
      val schema = SchemaEncoder[Foo].encode()
      RecordEncoder[Foo](schema).encode(Foo(true)) shouldBe InternalRecord(schema, Vector(true))
    }
    "encode floats" in {
      case class Foo(d: Float)
      val schema = SchemaEncoder[Foo].encode()
      RecordEncoder[Foo](schema).encode(Foo(123.435F)) shouldBe InternalRecord(schema, Vector(123.435F))
    }
  }
}


