//package com.sksamuel.avro4s.github
//
//import com.sksamuel.avro4s.AvroSchema
//import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.matchers.should.Matchers
//
//class GithubIssue330 extends AnyFunSuite with Matchers {
//
//  test("Unable to generate schema for a CoProduct where on of the case class has a parameter of type Map[String, String]") {
//    val schema = AvroSchema[Foo330]
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github/github_330.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }
//}
//
//sealed trait KeySetting
//final case class ForeignKeySetting(targetEntityCode: String,
//                                   targetFieldName: String) extends KeySetting
//final case class PrimaryKeySetting(attributes: Map[String, Int]) extends KeySetting
//
//case class Foo330(values: List[KeySetting])