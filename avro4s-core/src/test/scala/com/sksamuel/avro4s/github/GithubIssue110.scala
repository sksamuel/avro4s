package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.RecordFormat
import org.scalatest.{FunSuite, Matchers}

case class P1(name: String, age: Int = 18)
case class P2(name: String)

class GithubIssue110 extends FunSuite with Matchers {

  test("default value should be picked up") {
    val f1 = RecordFormat[P1]
    val f2 = RecordFormat[P2]
    f1.from(f2.to(P2("foo"))) shouldBe P1("foo")
  }
}
