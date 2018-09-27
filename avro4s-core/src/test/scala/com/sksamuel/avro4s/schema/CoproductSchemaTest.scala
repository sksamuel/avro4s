package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.AvroSchema
import org.scalatest.{FunSuite, Matchers}
import shapeless.{:+:, CNil}

class CoproductSchemaTest extends FunSuite with Matchers {

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
}

case class Gimble(x: String)
case class CPWrapper(u: CPWrapper.ISBG)
case class CPWithOption(u: Option[CPWrapper.ISBG])
object CPWrapper {
  type ISBG = Int :+: String :+: Boolean :+: Gimble :+: CNil
}

case class Coproducts(cp: Int :+: String :+: Boolean :+: CNil)
case class CoproductOfCoproducts(cp: (Int :+: String :+: CNil) :+: Boolean :+: CNil)

