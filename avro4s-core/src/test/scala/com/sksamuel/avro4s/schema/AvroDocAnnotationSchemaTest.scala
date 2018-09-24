package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroDoc
import com.sksamuel.avro4s.internal.AvroSchema
import org.scalatest.{Matchers, WordSpec}

class DocAnnotationSchemaTest extends WordSpec with Matchers {

  "@AvroDoc" should {
    "support doc annotation on class" in {
      @AvroDoc("hello; is it me youre looking for") case class Annotated(str: String)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/doc_annotation_class.avsc"))
      val schema = AvroSchema[Annotated]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support doc annotation on field and class" in {
      case class Annotated(@AvroDoc("hello its me") str: String, @AvroDoc("I am a long") long: Long, int: Int)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/doc_annotation_field.avsc"))
      val schema = AvroSchema[Annotated]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support doc annotation on nested class" in {
      case class Nested(@AvroDoc("b") foo: String)
      case class Annotated(@AvroDoc("c") nested: Nested)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/doc_annotation_field_struct.json"))
      val schema = AvroSchema[Annotated]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support doc on value types used as nested classes" in {
      case class Annotated(a: ValueTypeForDocAnnoTest)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/doc_annotation_value_type.json"))
      val schema = AvroSchema[Annotated]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

@AvroDoc("wibble")
case class ValueTypeForDocAnnoTest(s: String) extends AnyVal