package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData
import org.scalatest.{Matchers, WordSpec}

class AvroFixedTest extends WordSpec with Matchers {
  val m = AvroMessage(
    QuarterSHA256(new scala.collection.mutable.WrappedArray.ofByte(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7))),
    Array[Byte](0, 1, 2, 3))

  "AvroFixed" should {
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
    "support usage on strings" in {
      val schema = SchemaFor[FixedString]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/fixed_string.json"))
      schema.toString(true) shouldBe expected.toString(true)

      val fmt = RecordFormat[FixedString]
      val a = fmt.to(FixedString("sam"))
      a.get("mystring").asInstanceOf[GenericData.Fixed].bytes().toVector shouldBe Vector[Byte](115, 97, 109)
    }
  }
}

case class FixedString(@AvroFixed(7) mystring: String)

@AvroFixed(8)
case class QuarterSHA256(bytes: scala.collection.mutable.WrappedArray.ofByte) extends AnyVal

case class AvroMessage(schema: QuarterSHA256, payload: Array[Byte])
