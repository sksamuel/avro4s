package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroFixed
import com.sksamuel.avro4s.internal.AvroSchema
import org.apache.avro.Schema.Type
import org.scalatest.{Matchers, WordSpec}

class AvroFixedSchemaTest extends WordSpec with Matchers {

  "@AvroFixed" should {
    "generate fixed schema when used on top level class" in {
      val schema = AvroSchema[FixedValueClass]
      schema.getType shouldBe Type.FIXED
      schema.getFixedSize shouldBe 8
    }
    "generated fixed field schema when used on a field" in {
      val schema = AvroSchema[FixedString]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/fixed_string.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class FixedString(@AvroFixed(7) mystring: String)

@AvroFixed(8)
case class FixedValueClass(bytes: Array[Byte]) extends AnyVal