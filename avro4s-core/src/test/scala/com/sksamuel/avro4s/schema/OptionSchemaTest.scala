package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.{FunSuite, Matchers}
import shapeless.{:+:, CNil}

class OptionSchemaTest extends FunSuite with Matchers {

  test("generate option as Union[T, Null]") {
    case class Test(option: Option[String])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support mixing optionals with unions, merging appropriately") {
    val outsideOptional = AvroSchema[OptionalUnion]
    val insideOptional = AvroSchema[UnionOfOptional]
    val bothOptional = AvroSchema[AllOptionals]

    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optional_union.json"))

    outsideOptional.toString(true) shouldBe expected.toString(true)
    insideOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "UnionOfOptional")
    bothOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "AllOptionals")
  }

  test("move default option values to first schema as per avro spec") {
    val schema = AvroSchema[OptionWithDefault]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option_default_value.json"))
    schema.toString(true) shouldBe expected.toString(true)
}
}

case class OptionWithDefault(name: Option[String] = Some("f"))

case class OptionalUnion(union: Option[Int :+: String :+: CNil])
case class UnionOfOptional(union: Option[Int] :+: String :+: CNil)
case class AllOptionals(union: Option[Option[Int] :+: Option[String] :+: CNil])