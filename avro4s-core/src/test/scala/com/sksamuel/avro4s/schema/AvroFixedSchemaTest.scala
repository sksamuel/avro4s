package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroFixed, AvroSchema}
import org.scalatest.{Matchers, WordSpec}

class AvroFixedSchemaTest extends WordSpec with Matchers {

  "@AvroFixed" should {

    "generated fixed field schema when used on a field" in {
      val schema = AvroSchema[FixedStringField]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/fixed_string.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }

    "generated fixed schema when an annotated value type is used as the type in a field" in {
      val schema = AvroSchema[FooWithValue]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/fixed_string_value_type_as_field.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }

    "generate fixed schema for an annotated top level value type" in {
      val schema = AvroSchema[FixedValueClass]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/fixed_string_top_level_value_type.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class FixedStringField(@AvroFixed(7) mystring: String)

case class FooWithValue(z: FixedValueClass)

@AvroFixed(8)
case class FixedValueClass(bytes: Array[Byte]) extends AnyVal