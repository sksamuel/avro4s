package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.AvroFixed
import org.scalatest.{Matchers, WordSpec}

class AvroFixedTest extends WordSpec with Matchers {

  "@AvroFixed" should {
//    "encode byte array field annotated with fixed(n) as a plain vector of bytes with fixed length" in {
//      val schema = SchemaEncoder[FooWithFixedByteArray]
//      Encoder[FooWithFixedByteArray](schema).encode(FooWithFixedByteArray(Array[Byte](1, 2, 3))) shouldBe
//        InternalRecord(schema, Vector(Vector(1, 2, 3)))
//    }
//    "encode string field annotated with fixed(n) as a plain vector of bytes with fixed length" in {
//      val schema = SchemaEncoder[FooWithFixedString]
//      Encoder[FooWithFixedString](schema).encode(FooWithFixedString("hello")) shouldBe
//        InternalRecord(schema, Vector("hello".getBytes("UTF-8").array.toVector))
//    }
    //    "encode value type annotated with fixed(n) as an instance of GenericData.Fixed" in {
    //
    //      val schema = SchemaEncoder[FixedValueType]
    //      val bytes = Array[Byte](1, 2, 3)
    //
    //      val expected = new GenericData.Fixed(schema)
    //      expected.bytes(bytes)
    //
    //      RecordEncoder[FixedValueType](schema).encode(FixedValueType(bytes)) shouldBe expected
    //    }
    //    "support fixed strings" in {
    //      val schema = SchemaEncoder[FooWithFixedString]
    //      RecordEncoder[FooWithFixedString](schema).encode(FooWithFixedString("hi")) shouldBe
    //        InternalRecord(schema, Vector("hello"))
    //    }
  }
}

case class FooWithFixedByteArray(@AvroFixed(7) payload: Array[Byte])

case class FooWithFixedString(@AvroFixed(8) payload: String)

@AvroFixed(6)
case class FixedValueType(payload: Array[Byte]) extends AnyVal

