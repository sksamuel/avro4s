package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ArraySchemasTest extends AnyFunSuite with Matchers {

  test("array of longs") {
    case class Test(a: Array[Long])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/arrays/array_longs.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("array of ints") {
    case class Test(a: Array[Int])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/arrays/array_ints.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("array of strings") {
    case class Test(a: Array[String])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/arrays/array_strings.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("array of booleans") {
    case class Test(a: Array[Boolean])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/arrays/array_booleans.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("array of doubles") {
    case class Test(a: Array[Double])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/arrays/array_doubles.json"))
    val schema = AvroSchema[Test]
    schema.toString(true) shouldBe expected.toString(true)
  }

  //  test("array of classes") {
  //    case class Nested(goo: String)
  //    case class Test(seq: Array[Nested])
  //    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/seqrecords.json"))
  //    val schema = AvroSchema[Test]
  //    schema.toString(true) shouldBe expected.toString(true)
  //  }

  //  test("array of tuples") {
  //    case class Nested(goo: String)
  //    case class Test(seq: Array[Tuple2[String, Boolean]])
  //    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/seqrecords.json"))
  //    val schema = AvroSchema[Test]
  //    schema.toString(true) shouldBe expected.toString(true)
  //  }
}
