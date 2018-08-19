package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{InternalRecord, RecordEncoder, SchemaEncoder}
import org.scalatest.{Matchers, WordSpec}

class StructEncoderTest extends WordSpec with Matchers {

  "RecordEncoder" should {
    "encode structs" in {
      case class Foo(s: String, l: Long, b: Boolean, d: Double)
      val schema = SchemaEncoder[Foo].encode()
      RecordEncoder[Foo](schema).encode(Foo("a", 1, true, 0.3)) shouldBe InternalRecord(schema, Vector("a", 1, true, 0.3))
    }
  }
}
