package com.sksamuel.avro4s.github

import org.scalatest.{FunSuite, Matchers}
import com.sksamuel.avro4s._

object Issue234 {
  case class TestEntry(name: String)
  case class Test(id: Int, entries: List[TestEntry])
  val format: RecordFormat[Test] = RecordFormat[Test]
}

class GithubIssue234 extends FunSuite with Matchers {

  test("RecordFormat macro for List - diverging implicit expansion for type #234") {
    println(Issue234.format)
  }
}
