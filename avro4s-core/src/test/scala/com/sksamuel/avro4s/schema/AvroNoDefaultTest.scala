package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroNoDefault, AvroSchema}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AvroNoDefaultTest extends AnyFunSuite with Matchers {
  test("a field annotated with @AvroNoDefault should ignore a scala default") {
    val schema = AvroSchema[NoDefaultTest]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_nodefault.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }
}

case class NoDefaultTest(@AvroNoDefault a: String = "foowoo")
