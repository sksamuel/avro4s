package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema}
import org.scalatest.funsuite.AnyFunSuite

class BasicSchemasTest extends AnyFunSuite {

  test("boolean SchemaFor") {
    case class Testy(booly: Boolean)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/boolean.json"))
    val schema = AvroSchema[Testy]
    assert(schema.toString(true) == expected.toString(true))
  }

  test("byteArray SchemaFor") {
    case class Testy(bytes: Array[Byte])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bytes.json"))
    val schema = AvroSchema[Testy]
    assert(schema.toString(true) == expected.toString(true))
  }

  test("string SchemaFor") {
    case class Testy(str: String)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/string.json"))
    val schema = AvroSchema[Testy]
    assert(schema.toString(true) == expected.toString(true))
  }

  test("int SchemaFor") {
    case class Testy(inty: Int)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/integer.json"))
    val schema = AvroSchema[Testy]
    assert(schema.toString(true) == expected.toString(true))
  }

  test("long SchemaFor") {
    case class Testy(longy: Long)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/long.json"))
    val schema = AvroSchema[Testy]
    assert(schema.toString(true) == expected.toString(true))
  }

  test("double SchemaFor") {
    case class Testy(double: Double)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/double.json"))
    val schema = AvroSchema[Testy]
    assert(schema.toString(true) == expected.toString(true))
  }

  test("float SchemaFor") {
    case class Testy(float: Float)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/float.json"))
    val schema = AvroSchema[Testy]
    assert(schema.toString(true) == expected.toString(true))
  }
}

case class Level4(str: Map[String, String])
case class Level3(level4: Level4)
case class Level2(level3: Level3)
case class Level1(level2: Level2)