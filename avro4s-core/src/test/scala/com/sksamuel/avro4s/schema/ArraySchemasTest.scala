package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ArraySchemasTest extends AnyFunSuite with Matchers {

  test("array of longs") {
    case class Test(a: Array[Long])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/arrays/longs.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("array of ints") {
    case class Test(a: Array[Int])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/arrays/ints.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("array of strings") {
    case class Test(a: Array[String])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/arrays/strings.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("array of booleans") {
    case class Test(a: Array[Boolean])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/arrays/booleans.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("array of doubles") {
    case class Test(a: Array[Double])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/arrays/doubles.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("array of classes") {
    case class Nested(b: String, c: Int)
    case class Test(a: Array[Nested])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/arrays/classes.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("array of tuples") {
    case class Test(a: Array[Tuple2[String, Boolean]])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/arrays/tuples.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }
}
