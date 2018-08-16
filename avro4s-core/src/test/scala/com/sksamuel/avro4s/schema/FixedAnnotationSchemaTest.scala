package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroFixed, SchemaFor}
import org.apache.avro.Schema.Type
import org.scalatest.{Matchers, WordSpec}

class AvroFixedSchemaTest extends WordSpec with Matchers {

  "AvroFixed" should {
    "generate fixed(n) type for @AvroFixed(n) case class" in {
      val schema = SchemaFor[QuarterSHA256]()
      schema.getType shouldBe Type.FIXED
      schema.getFixedSize shouldBe 8
    }
    "support usage on strings" in {
      val schema = SchemaFor[FixedString]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/fixed_string.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class FixedString(@AvroFixed(7) mystring: String)

@AvroFixed(8)
case class QuarterSHA256(bytes: scala.collection.mutable.WrappedArray.ofByte) extends AnyVal

case class AvroMessage(schema: QuarterSHA256, payload: Array[Byte])