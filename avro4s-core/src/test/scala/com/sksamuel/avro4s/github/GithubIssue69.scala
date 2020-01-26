package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.AvroSchemaV2
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.higherKinds

case class Message[T](payload: T, identity: String = "5b16ca84-f9e1-46a9-bc5c-8c0052b6dd16")
case class MyRecord(key: String, str1: String, str2: String, int1: Int)

class GithubIssue69 extends AnyFunSuite with Matchers {

  test("Can't create schema for generic type #69") {
    val schema = AvroSchemaV2[Message[MyRecord]]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github69.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }
}
