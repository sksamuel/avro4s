package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.SchemaFor
import org.scalatest.{Matchers, WordSpec}
import shapeless.{:+:, CNil}

class OptionSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "generate option as Union[T, Null]" in {
      case class Test(option: Option[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option.json"))
      val schema = SchemaFor[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support mixing optionals with unions, merging appropriately" in {
      val outsideOptional = SchemaFor[OptionalUnion]
      val insideOptional = SchemaFor[UnionOfOptional]
      val bothOptional = SchemaFor[AllOptionals]

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optionalunion.json"))

      outsideOptional.toString(true) shouldBe expected.toString(true)
      insideOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "UnionOfOptional")
      bothOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "AllOptionals")
    }
    "move default option values to first schema as per avro spec" in {
      val schema = SchemaFor[OptionWithDefault]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optiondefaultvalues.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class OptionWithDefault(name: Option[String] = Some("f"))

case class Union(union: Int :+: String :+: Boolean :+: CNil)
case class OptionalUnion(union: Option[Int :+: String :+: CNil])
case class UnionOfOptional(union: Option[Int] :+: String :+: CNil)
case class AllOptionals(union: Option[Option[Int] :+: Option[String] :+: CNil])