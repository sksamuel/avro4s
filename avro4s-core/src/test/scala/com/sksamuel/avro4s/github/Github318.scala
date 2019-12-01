package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{DefaultFieldMapper, SchemaFor}
import shapeless.{:+:, CNil}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

sealed trait MyAdt
object MyAdt {
  case class Foo(name: String) extends MyAdt
  case class Bar(id: Int) extends MyAdt
}
case class CoproductWithAdt(cp: MyAdt :+: Boolean :+: CNil)

class Github318 extends AnyFunSuite with Matchers {

  test("Error getting SchemaFor instance for Coproduct with ADT #318") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github/github_318.json"))
    SchemaFor[CoproductWithAdt].schema(DefaultFieldMapper).toString(true) shouldBe expected.toString(true)
  }
}
