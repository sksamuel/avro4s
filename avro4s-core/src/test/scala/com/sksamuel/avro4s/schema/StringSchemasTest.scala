package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchema
import com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StringSchemasTest extends AnyWordSpec with Matchers {

  "SchemaEncoder" should {
    "accept strings" in {
      case class Test(str: String)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/string.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}