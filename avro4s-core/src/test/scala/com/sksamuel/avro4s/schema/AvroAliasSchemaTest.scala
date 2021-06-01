package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroAlias, AvroSchema}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AvroAliasSchemaTest extends AnyWordSpec with Matchers {

  "SchemaEncoder" should {
    "support alias annotations on types" in {

      @AvroAlias("queen")
      case class Annotated(str: String)

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/aliases_on_types.json"))
      val schema = AvroSchema[Annotated]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support multiple alias annotations on types" in {

      @AvroAlias("queen")
      @AvroAlias("ledzep")
      case class Annotated(str: String)

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/aliases_on_types_multiple.json"))
      val schema = AvroSchema[Annotated]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support alias annotations on field" in {

      case class Annotated(@AvroAlias("cold") str: String, @AvroAlias("kate") @AvroAlias("bush") long: Long, int: Int)

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/aliases_on_fields.json"))
      val schema = AvroSchema[Annotated]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}
