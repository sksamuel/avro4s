package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{AvroSchema, Decoder, Encoder}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

case class B(b: Map[String, String])

case class A(a: Seq[B])

class GithubIssue247 extends AnyFunSuite with Matchers {
  test("Diverging implicit expansion error on case class #247") {
    AvroSchema[A]
    Encoder[A]
    Decoder[A]
  }
}
