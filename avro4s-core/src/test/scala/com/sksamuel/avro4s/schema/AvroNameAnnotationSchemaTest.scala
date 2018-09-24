package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroName
import com.sksamuel.avro4s.internal.AvroSchema
import org.scalatest.{Matchers, WordSpec}

class AvroNameAnnotationSchemaTest extends WordSpec with Matchers {
  "SchemaEncoder" should {
    "generate field names using @AvroName" in {
      case class Foo(@AvroName("wibble") wobble: String, wubble: String)
      val schema = AvroSchema[Foo]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avroname.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}
