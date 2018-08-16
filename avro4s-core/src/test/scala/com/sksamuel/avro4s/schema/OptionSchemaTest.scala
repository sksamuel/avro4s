package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s._
import org.scalatest.{Matchers, WordSpec}
import shapeless.{:+:, CNil}

class OptionSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "generate option as Union[T, Null]" in {
      case class Test(option: Option[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default option values" in {
      val schema = SchemaFor[OptionDefaultValues]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optiondefaultvalues.avsc"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support mixing optionals with unions, merging appropriately" in {
      val outsideOptional = SchemaFor[OptionalUnion]()
      val insideOptional = SchemaFor[UnionOfOptional]()
      val bothOptional = SchemaFor[AllOptionals]()

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optionalunion.avsc"))

      outsideOptional.toString(true) shouldBe expected.toString(true)
      insideOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "UnionOfOptional")
      bothOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "AllOptionals")
    }
  }
}

case class OptionDefaultValues(name: String = "sammy",
                               description: Option[String] = None,
                               currency: Option[String] = Some("$"))

case class Union(union: Int :+: String :+: Boolean :+: CNil)
case class OptionalUnion(union: Option[Int :+: String :+: CNil])
case class UnionOfOptional(union: Option[Int] :+: String :+: CNil)
case class AllOptionals(union: Option[Option[Int] :+: Option[String] :+: CNil])
