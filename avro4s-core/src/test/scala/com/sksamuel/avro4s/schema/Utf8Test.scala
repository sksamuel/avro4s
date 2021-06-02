package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema}
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class Utf8Test extends AnyFunSuite with Matchers {

  test("utf8 fields") {
    case class Person(name: Utf8, alias: Utf8, age: Int)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/utf8.json"))
    val schema = AvroSchema[Person]
    schema.toString(true) shouldBe expected.toString(true)
  }

}
