package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OptionSchemaTest extends AnyFunSuite with Matchers {

  test("generate option of either Union[Null, A, B]") {
    case class Test(option: Option[Either[String, Boolean]])

    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option_either.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate option of sealed traits enums as Union[Null, {enum}]") {
    sealed trait T
    case object A extends T
    case object B extends T
    case object C extends T
    case class Test(option: Option[T])

    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option_enum.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate option of sealed traits as Union[Null, A, B, C]") {
    sealed trait T
    case class A(a: String) extends T
    case class B(b: String) extends T
    case class C(c: String) extends T
    case class Test(option: Option[T])

    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option_sealed_trait.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate option as Union[Null, T]") {
    case class Test(option: Option[String])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support none") {
    case class Test(option: None.type)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/none.json"))
    val schema = AvroSchema[Test]
    schema shouldBe expected
  }

  //  test("support mixing optionals with unions, merging appropriately") {
  //    val outsideOptional = AvroSchema[OptionalUnion]
  //    val insideOptional = AvroSchema[UnionOfOptional]
  //    val bothOptional = AvroSchema[AllOptionals]
  //
  //    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optional_union.json"))
  //
  //    outsideOptional.toString(true) shouldBe expected.toString(true)
  //    insideOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "UnionOfOptional")
  //    bothOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "AllOptionals")
  //  }

  // todo requires defaults
  // test("move default option values to first schema as per avro spec") {
  //    val schema = AvroSchema[OptionWithDefault]
  //    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option_default_value.json"))
  //    schema.toString(true) shouldBe expected.toString(true)
  // }

  // todo requires defaults
  // test("if a field has a default value of null then define the field to be nullable") {
  //    val schema = AvroSchema[FieldWithNull]
  //    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option_from_null_default.json"))
  //    schema.toString(true) shouldBe expected.toString(true)
  // }
}

case class OptionWithDefault(name: Option[String] = Some("f"))
case class FieldWithNull(name: String = null)

//case class OptionalUnion(union: Option[Int :+: String :+: CNil])
//case class UnionOfOptional(union: Option[Int] :+: String :+: CNil)
//case class AllOptionals(union: Option[Option[Int] :+: Option[String] :+: CNil])