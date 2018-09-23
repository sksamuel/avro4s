package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.SchemaEncoder
import org.scalatest.{Matchers, WordSpec}
import shapeless.{:+:, CNil}

class OptionSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "generate option as Union[T, Null]" in {
      case class Test(option: Option[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option.json"))
      val schema = SchemaEncoder[Test].encode()
      schema.toString(true) shouldBe expected.toString(true)
    }

    "support mixing optionals with unions, merging appropriately" in {
      val outsideOptional = SchemaEncoder[OptionalUnion].encode()
      val insideOptional = SchemaEncoder[UnionOfOptional].encode()
      val bothOptional = SchemaEncoder[AllOptionals].encode()

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optionalunion.json"))

      outsideOptional.toString(true) shouldBe expected.toString(true)
      insideOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "UnionOfOptional")
      bothOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "AllOptionals")
    }
    "support default option values" in {
      //val schema = SchemaFor[OptionDefaultValues]()
      //val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optiondefaultvalues.avsc"))
      //schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class Union(union: Int :+: String :+: Boolean :+: CNil)
case class OptionalUnion(union: Option[Int :+: String :+: CNil])
case class UnionOfOptional(union: Option[Int] :+: String :+: CNil)
case class AllOptionals(union: Option[Option[Int] :+: Option[String] :+: CNil])

case class OptionDefaultValues(name: String = "sammy",
                               description: Option[String] = None,
                               currency: Option[String] = Some("$"))
