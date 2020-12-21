package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroDoc, AvroError, AvroSchema}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AvroErrorTest extends AnyWordSpec with Matchers {

  "@AvroError" should {
    "support annotation on class" in {
      
      @AvroError
      case class ErrorRecord(str: String)
      
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/avroerror/class.json"))
      val schema = AvroSchema[ErrorRecord]
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support annotation on nested classes" in {

      @AvroError
      case class ErrorRecord(str: String)

      case class Container(e: ErrorRecord)

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/avroerror/nested.json"))
      val schema = AvroSchema[Container]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}