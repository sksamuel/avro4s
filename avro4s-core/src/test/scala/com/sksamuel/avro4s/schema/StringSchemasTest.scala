package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema, Encoder, ImmutableRecord, SchemaFor}
import com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage
import com.sksamuel.avro4s.schemas.JavaStringSchemaFor
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
    "encode strings as java strings when JavaStringSchemaFor is in scope" in {
      case class Foo(s: String)
      given SchemaFor[String] = JavaStringSchemaFor
      val schema = AvroSchema[Foo]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/java_string.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}