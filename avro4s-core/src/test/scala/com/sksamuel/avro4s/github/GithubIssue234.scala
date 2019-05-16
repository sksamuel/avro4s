//package com.sksamuel.avro4s.github
//
//import com.sksamuel.avro4s._
//import org.scalatest.{FunSuite, Matchers}
//
//case class TestEntry(name: String)
//sealed trait TestClass // class or trait
//final case class Test(id: Int, entries: List[TestEntry]) extends TestClass // <-- list
//case class ContainsTestClass(testClass: TestClass)
//
//class GithubIssue234 extends FunSuite with Matchers {
//
//  test("RecordFormat macro for List - diverging implicit expansion for type #234") {
//    val format: RecordFormat[ContainsTestClass] = RecordFormat[ContainsTestClass]
//  }
//}
