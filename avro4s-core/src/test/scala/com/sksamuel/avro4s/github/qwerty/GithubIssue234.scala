//package com.sksamuel.avro4s.github.qwerty
//
//import com.sksamuel.avro4s._
//import org.scalatest.{FunSuite, Matchers}
//
////sealed trait TestTrait
////case class TestEntry(name: String)
////abstract class WibbleAAAA extends TestTrait
////final case class Test(id: Int, entries: List[TestEntry]) extends WibbleAAAA
////case class ContainsTestTrait(testTrait: TestTrait)
//
//object Issue234 {
//  //val format: RecordFormat[ContainsTestTrait] = RecordFormat[ContainsTestTrait]
//}
//
//class GithubIssue234 extends FunSuite with Matchers {
//
//  test("RecordFormat macro for List - diverging implicit expansion for type #234") {
//
//    val schema: SchemaFor[TestTrait] = SchemaFor[TestTrait]
//    println(schema.schema.toString(true))
//
//    //val encoder: Encoder[ContainsTestTrait] = Encoder[ContainsTestTrait]
//    //println(encoder)
//  }
//}
