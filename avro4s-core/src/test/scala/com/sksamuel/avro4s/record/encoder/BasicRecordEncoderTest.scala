package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.RecordEncoder
import org.scalatest.{Matchers, WordSpec}

class BasicRecordEncoderTest extends WordSpec with Matchers {

  "RecordEncoder" should {
    "encode strings" in {
      case class Foo(s: String)
      RecordEncoder[Foo].encode(Foo("hello")) shouldBe ""
    }
    "encode longs" in {
      case class Foo(l: Long)
      RecordEncoder[Foo].encode(Foo(124345L)) shouldBe ""
    }
    "encode doubles" in {
      case class Foo(d: Double)
      RecordEncoder[Foo].encode(Foo(123.456)) shouldBe ""
    }
  }
}
