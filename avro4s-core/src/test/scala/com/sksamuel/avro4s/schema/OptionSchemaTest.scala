package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OptionSchemaTest extends AnyFunSuite with Matchers {

  test("generate option as Union[T, Null]") {
    case class Test(option: Option[String])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  //  test("support mixing optionals with unions, merging appropriately") {
  //    val outsideOptional = AvroSchemaV2[OptionalUnion]
  //    val insideOptional = AvroSchemaV2[UnionOfOptional]
  //    val bothOptional = AvroSchemaV2[AllOptionals]
  //
  //    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optional_union.json"))
  //
  //    outsideOptional.toString(true) shouldBe expected.toString(true)
  //    insideOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "UnionOfOptional")
  //    bothOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "AllOptionals")
  //  }

  test("move default option values to first schema as per avro spec") {
    val schema = AvroSchema[OptionWithDefault]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option_default_value.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("if a field has a default value of null then define the field to be nullable") {
    val schema = AvroSchema[FieldWithNull]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option_from_null_default.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }
}

case class OptionWithDefault(name: Option[String] = Some("f"))
case class FieldWithNull(name: String = null)

//case class OptionalUnion(union: Option[Int :+: String :+: CNil])
//case class UnionOfOptional(union: Option[Int] :+: String :+: CNil)
//case class AllOptionals(union: Option[Option[Int] :+: Option[String] :+: CNil])