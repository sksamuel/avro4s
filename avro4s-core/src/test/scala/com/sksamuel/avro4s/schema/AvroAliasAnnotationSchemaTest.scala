package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroAlias
import com.sksamuel.avro4s.internal.AvroSchema
import org.scalatest.{Matchers, WordSpec}

class AvroAliasAnnotationSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "support alias annotations on types" in {

      @AvroAlias("queen")
      case class Annotated(str: String)

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/aliases_on_types.avsc"))
      val schema = AvroSchema[Annotated]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support multiple alias annotations on types" in {

      @AvroAlias("queen")
      @AvroAlias("ledzep")
      case class Annotated(str: String)

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/aliases_on_types_multiple.avsc"))
      val schema = AvroSchema[Annotated]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support alias annotations on field" in {

      case class Annotated(@AvroAlias("cold") str: String, @AvroAlias("kate") @AvroAlias("bush") long: Long, int: Int)

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/aliases_on_fields.avsc"))
      val schema = AvroSchema[Annotated]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}
