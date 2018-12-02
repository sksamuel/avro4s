package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{AvroSchema, Encoder}
import org.scalatest.{FunSuite, Matchers}

case class B(b: Map[String, String])

case class A(a: List[B])

class GithubIssue247 extends FunSuite with Matchers {
  test("Diverging implicit expansion error on case class ") {
    println(AvroSchema[A])
    println(Encoder[A])
  }
}
