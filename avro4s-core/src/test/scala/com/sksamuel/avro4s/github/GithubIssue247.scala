package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{AvroSchema, Decoder, Encoder}
import org.scalatest.{FunSuite, Matchers}

case class B(b: Map[String, String])

case class A(a: Seq[B])

class GithubIssue247 extends FunSuite with Matchers {
  ignore("Diverging implicit expansion error on case class ") {
//    AvroSchema[A]
//    Encoder[A]
//    Decoder[A]
  }
}
