package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{AvroSchema, FromRecord, ToRecord}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

case class P1(name: String, age: Int = 18)
case class P2(name: String)

class GithubIssue110 extends AnyFunSuite with Matchers {

  test("default value should be picked up") {
    val f1 = FromRecord[P1](AvroSchema[P1])
    val f2 = ToRecord[P2](AvroSchema[P2])
    f1.from(f2.to(P2("foo"))) shouldBe P1("foo")
  }
}
