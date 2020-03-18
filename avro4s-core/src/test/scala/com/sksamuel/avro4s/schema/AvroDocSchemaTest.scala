package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroDoc, AvroSchemaV2}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AvroDocSchemaTest extends AnyWordSpec with Matchers {

  "@AvroDoc" should {
    "support doc annotation on class" in {
      @AvroDoc("hello; is it me youre looking for") case class Annotated(str: String)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/doc_annotation_class.avsc"))
      val schema = AvroSchemaV2[Annotated]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support doc annotation on field and class" in {
      case class Annotated(@AvroDoc("hello its me") str: String, @AvroDoc("I am a long") long: Long, int: Int)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/doc_annotation_field.avsc"))
      val schema = AvroSchemaV2[Annotated]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support doc annotation on nested class" in {
      case class Nested(@AvroDoc("b") foo: String)
      case class Annotated(@AvroDoc("c") nested: Nested)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/doc_annotation_field_struct.json"))
      val schema = AvroSchemaV2[Annotated]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support value type when placed at the class level should annotate the field in the final schema" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/doc_annotation_value_type.json"))
      val schema = AvroSchemaV2[Annotated123]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class Annotated123(a: ValueTypeForDocAnnoTest)
@AvroDoc("wibble")
case class ValueTypeForDocAnnoTest(s: String) extends AnyVal