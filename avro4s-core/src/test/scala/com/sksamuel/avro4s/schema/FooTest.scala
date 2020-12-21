//package com.sksamuel.avro4s.schema
//
//import com.sksamuel.avro4s.{AvroSchema, Print}
//import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.matchers.should.Matchers
//
//class FooTest extends AnyFunSuite with Matchers {
//
//  test("foo") {
//    val r = Print[Starship]
//    r.print shouldBe "a"
//  }
//}
//
//case class Starship(shipname: String, registry: Registry)
//
//case class Registry(ncc: Recursive)
//
//case class Recursive(a: Recursive)