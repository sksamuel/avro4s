package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{AvroSchema, FromRecord, SchemaFor, ToRecord}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GithubIssue885 extends AnyFunSuite with Matchers {

  test("support Option of sealed trait with 1 subtype") {
    sealed trait Example
    case class A(v: String) extends Example

    val value = Some(A("a"))

    val schema = AvroSchema[Option[Example]]
    val record = ToRecord[Option[Example]](schema).to(value)
    FromRecord[Option[Example]](schema).from(record) shouldBe value
  }
}
