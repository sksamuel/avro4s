package com.sksamuel.avro4s.record.encoder

import org.scalatest.{Matchers, WordSpec}

class BasicRecordEncoderTest extends WordSpec with Matchers {

  "RecordEncoder" should {
    "encode strings" in {
      case class Foo(s: String)
     // RecordEncoder[Foo].encode(Foo("hello")) shouldBe Record(SchemaEncoder[Foo].encode, Array.empty[AnyRef])
      //    RecordEncoder[Foo].encode(Foo("hello")) shouldBe Record(SchemaEncoder[Foo].encode, Array("qwe"))
    }
    "encode longs" in {
      case class Foo(l: Long)
    //  RecordEncoder[Foo].encode(Foo(124345L)) shouldBe Record(SchemaEncoder[Foo].encode, Array.empty[AnyRef])
      //  RecordEncoder[Foo].encode(Foo(124345L)) shouldBe Record(SchemaEncoder[Foo].encode, Array(java.lang.Long.valueOf(124345L)))
    }
    "encode doubles" in {
      case class Foo(d: Double)
    //  RecordEncoder[Foo].encode(Foo(123.456)) shouldBe Record(SchemaEncoder[Foo].encode, Array.empty[AnyRef])
      //    RecordEncoder[Foo].encode(Foo(123.456)) shouldBe Record(SchemaEncoder[Foo].encode, Array(java.lang.Double.valueOf(123.456)))
    }
  }
}
