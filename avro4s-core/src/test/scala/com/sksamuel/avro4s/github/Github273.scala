package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.SchemaFor
import org.scalatest.{FunSuite, Matchers}
import shapeless.{:+:, CNil}

case class Coproducts(cp: Int :+: String :+: Boolean :+: CNil)
case class CoproductOfCoproductsField(cp: Coproducts :+: Boolean :+: CNil)

class Github273 extends FunSuite with Matchers {

  test("Diverging implicit expansion for SchemaFor in Coproducts inside case classes #273") {
    SchemaFor[CoproductOfCoproductsField].schema
  }
}
