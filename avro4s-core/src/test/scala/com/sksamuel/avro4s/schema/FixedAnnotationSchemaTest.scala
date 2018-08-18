package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroFixed
import com.sksamuel.avro4s.internal.SchemaEncoder
import org.apache.avro.Schema.Type
import org.scalatest.{Matchers, WordSpec}

class AvroFixedSchemaTest extends WordSpec with Matchers {

  "@AvroFixed" should {
    "generate fixed schema when used on top level class" in {
      val schema = SchemaEncoder[QuarterSHA256].encode()
      schema.getType shouldBe Type.FIXED
      schema.getFixedSize shouldBe 8
    }
    "support usage on strings" in {
      val schema = SchemaEncoder[FixedString].encode()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/fixed_string.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class FixedString(@AvroFixed(7) mystring: String)

@AvroFixed(8)
case class QuarterSHA256(bytes: Array[Byte]) extends AnyVal

case class AvroMessage(schema: QuarterSHA256, payload: Array[Byte])