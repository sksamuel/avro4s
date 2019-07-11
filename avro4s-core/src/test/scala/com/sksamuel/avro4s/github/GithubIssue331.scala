//package com.sksamuel.avro4s.github
//
//import com.sksamuel.avro4s.{AvroDoc, AvroSchema}
//import org.scalatest.{FunSuite, Matchers}
//import shapeless.{:+:, CNil}
//
//class GithubIssue331 extends FunSuite with Matchers {
//
//  test("AvroDoc annotation and shapeless coproducts #331") {
//    val schema = AvroSchema[MyCaseClass]
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github/github_331.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }
//}
//
//@AvroDoc("my doc")
//case class MyCaseClass(myCoproduct: String :+: Int :+: CNil)