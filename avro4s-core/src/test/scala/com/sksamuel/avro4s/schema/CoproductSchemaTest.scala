package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchemaV2
import shapeless.{:+:, CNil}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CoproductSchemaTest extends AnyFunSuite with Matchers {

  test("coproducts") {
    val schema = AvroSchemaV2[CPWrapper]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/coproduct.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("options of coproducts") {
    val schema = AvroSchemaV2[CPWithOption]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/coproduct_option.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support coproducts of coproducts") {
    val coproductOfCoproducts = AvroSchemaV2[CoproductOfCoproducts]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/coproduct_of_coproducts.json"))
    coproductOfCoproducts.toString(true) shouldBe expected.toString(true)
  }
}

case class Gimble(x: String)
case class CPWrapper(u: CPWrapper.ISBG)
case class CPWithOption(u: Option[CPWrapper.ISBG])
object CPWrapper {
  type ISBG = Int :+: String :+: Boolean :+: Gimble :+: CNil
}

case class Coproducts(cp: Int :+: String :+: Boolean :+: CNil)
case class CoproductOfCoproducts(cp: (Int :+: String :+: CNil) :+: Boolean :+: CNil)

