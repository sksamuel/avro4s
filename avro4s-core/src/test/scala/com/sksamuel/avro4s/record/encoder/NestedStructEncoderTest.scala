package com.sksamuel.avro4s.record.encoder

import org.scalatest.{Matchers, WordSpec}

class NestedStructEncoderTest extends WordSpec with Matchers {

  "RecordEncoder" should {
    "encode nested structs" in {
      //   val schema = SchemaEncoder[Outer]
      //    Encoder[Outer](schema).encode(Outer("a", Inner(1.2, true))) shouldBe InternalRecord(schema, Vector("a", Inner(1.2, true)))
    }
  }
}

case class Outer(a: String, inner: Inner)
case class Inner(d: Double, b: Boolean)