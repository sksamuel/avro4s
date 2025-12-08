package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{AvroSchema, FromRecord, ToRecord}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GithubIssue890 extends AnyFunSuite with Matchers {

  test("support nested default None") {
    case class Inner(a: Option[String] = None)
    case class Outer(b: Inner = Inner())

    val value = Outer()

    val schema = AvroSchema[Outer]
    val record = ToRecord[Outer](schema).to(value)
    FromRecord[Outer](schema).from(record) shouldBe value
  }
}

