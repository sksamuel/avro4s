package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TupleSchemaTest extends AnyFunSuite with Matchers {

  case class Test2(z: (String, Int))
  case class Test3(z: (String, Int, Boolean))
  case class Test4(z: (String, Int, Boolean, Double))
  case class Test5(z: (String, Int, Boolean, Double, Long))

  test("tuple 2 schema") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/tuple2.json"))
    val schema = AvroSchema[Test2]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("tuple 3 schema") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/tuple3.json"))
    val schema = AvroSchema[Test3]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("tuple 4 schema") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/tuple4.json"))
    val schema = AvroSchema[Test4]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("tuple 5 schema") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/tuple5.json"))
    val schema = AvroSchema[Test5]
    schema.toString(true) shouldBe expected.toString(true)
  }
}
