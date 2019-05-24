package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{DefaultNamingStrategy, SchemaFor}
import org.scalatest.{FunSuite, Matchers}
import shapeless.{:+:, CNil}

sealed trait MyAdt
object MyAdt {
  case class Foo(name: String) extends MyAdt
  case class Bar(id: Int) extends MyAdt
}
case class CoproductWithAdt(cp: MyAdt :+: Boolean :+: CNil)

class Github318 extends FunSuite with Matchers {

  test("Error getting SchemaFor instance for Coproduct with ADT #318") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github/github_318.json"))
    SchemaFor[CoproductWithAdt].schema.toString(true) shouldBe expected.toString(true)
  }
}
