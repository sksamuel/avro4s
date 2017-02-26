package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema.Type
import org.scalatest.{Matchers, WordSpec}

class FixedTypeTest extends WordSpec with Matchers {
  val m = AvroMessage(
    QuarterSHA256(new scala.collection.mutable.WrappedArray.ofByte(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7))),
    Array[Byte](0, 1, 2, 3))

  "Avro4s" should {
    "generate fixed(n) type for @AvroFixed(n) case class" in {
      val schema = SchemaFor[QuarterSHA256]()
      schema.getType shouldBe Type.FIXED
      schema.getFixedSize shouldBe 8
    }
    "encode fixed(n) as a plain vector of bytes with fixed length" in {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[AvroMessage](baos)
      output.write(m.copy(payload = Array.emptyByteArray))
      output.close()

      val bytes = baos.toByteArray

      bytes.length shouldBe 9
      bytes.take(8) shouldBe m.schema.bytes
      bytes(8) shouldBe 0
    }
    "encode and decode fixed(n)" in {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[AvroMessage](baos)
      output.write(m)
      output.close()

      val decoded = AvroInputStream.binary[AvroMessage](baos.toByteArray).iterator.next()

      decoded.schema shouldBe m.schema
      decoded.payload.toSeq shouldBe m.payload.toSeq
    }
  }
}

@AvroFixed(8)
case class QuarterSHA256(bytes: scala.collection.mutable.WrappedArray.ofByte) extends AnyVal

case class AvroMessage(schema: QuarterSHA256, payload: Array[Byte])
