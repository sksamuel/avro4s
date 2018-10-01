package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroFixed, AvroSchema}
import com.sksamuel.avro4s.Encoder
import org.apache.avro.generic.GenericRecord
import org.scalatest.{FunSuite, Matchers}

@AvroFixed(8)
case class QuarterSHA256(bytes: scala.collection.mutable.WrappedArray.ofByte) extends AnyVal

case class FixedString(@AvroFixed(7) mystring: String)

case class AvroMessage(schema: QuarterSHA256, payload: Array[Byte])

class FixedEncoderTest extends FunSuite with Matchers {

  val m = AvroMessage(
    QuarterSHA256(new scala.collection.mutable.WrappedArray.ofByte(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7))),
    Array[Byte](0, 1, 2, 3)
  )

  //  test("encode fixed(n) as a plain vector of bytes with fixed length") {
  //    val schema = AvroSchema[AvroMessage]
  //    Encoder[AvroMessage].encode(m, schema) shouldBe ""
  //  }

  test("support usage on strings") {
    val schema = AvroSchema[FixedString]
    val record = Encoder[FixedString].encode(FixedString("sam"), schema).asInstanceOf[GenericRecord]
    record.get("mystring").asInstanceOf[Array[Byte]].toVector shouldBe Vector(115, 97, 109)
  }
}

