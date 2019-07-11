package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{AvroName, AvroNamespace, AvroSchema}
import org.scalatest.{FunSuite, Matchers}

class GithubIssue346 extends FunSuite with Matchers {

  test("Enum annotations failing #346") {
    val schema = AvroSchema[MyEnum]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github/github_346.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }
}

@AvroNamespace("com.foo")
@AvroName("MyRenamedEnum")
sealed trait MyEnum
case object FOO extends MyEnum
case object BAR extends MyEnum
