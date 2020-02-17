package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchema
import shapeless.{:+:, Coproduct, CNil}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CoproductSchemaTest extends AnyFunSuite with Matchers {

  test("coproducts") {
    val schema = AvroSchema[CPWrapper]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/coproduct.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("options of coproducts") {
    val schema = AvroSchema[CPWithOption]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/coproduct_option.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support coproducts of coproducts") {
    val coproductOfCoproducts = AvroSchema[CoproductOfCoproducts]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/coproduct_of_coproducts.json"))
    coproductOfCoproducts.toString(true) shouldBe expected.toString(true)
  }

  test("coproducts with default arguments") {
    val schema = AvroSchema[CPWithDefault]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/coproduct_with_default.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }
}

case class Gimble(x: String)
case class CPWrapper(u: CPWrapper.ISBG)
case class CPWithOption(u: Option[CPWrapper.ISBG])
case class CPWithDefault(u: CPWrapper.ISBG = Coproduct[CPWrapper.ISBG](123))
object CPWrapper {
  type ISBG = Int :+: String :+: Boolean :+: Gimble :+: CNil
}

case class Coproducts(cp: Int :+: String :+: Boolean :+: CNil)
case class CoproductOfCoproducts(cp: (Int :+: String :+: CNil) :+: Boolean :+: CNil)

