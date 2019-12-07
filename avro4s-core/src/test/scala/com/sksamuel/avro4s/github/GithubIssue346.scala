package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{AvroName, AvroNamespace, AvroSchema}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GithubIssue346 extends AnyFunSuite with Matchers {

  test("Enum annotations failing #346") {
    val schema = AvroSchema[MyEnum]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github/github_346.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }
}

@AvroNamespace("com.foo")
@AvroName("MyRenamedEnum")
sealed trait MyEnum
case object Hussel extends MyEnum
case object Bussel extends MyEnum
