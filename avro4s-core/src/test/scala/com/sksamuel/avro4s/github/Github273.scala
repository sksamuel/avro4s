//package com.sksamuel.avro4s.github
//
//import com.sksamuel.avro4s.SchemaFor
//import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.matchers.should.Matchers
//import shapeless.{:+:, CNil}
//
//case class Coproducts(cp: Int :+: String :+: Boolean :+: CNil)
//case class CoproductOfCoproductsField(cp: Coproducts :+: Boolean :+: CNil)
//
//class Github273 extends AnyFunSuite with Matchers {
//
//  test("Diverging implicit expansion for SchemaFor in Coproducts inside case classes #273") {
//    SchemaFor[CoproductOfCoproductsField]
//  }
//}
