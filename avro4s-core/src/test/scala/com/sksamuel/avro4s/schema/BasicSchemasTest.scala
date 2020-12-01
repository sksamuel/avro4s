//package com.sksamuel.avro4s.schema
//
//import com.sksamuel.avro4s.{AvroSchema}
//import org.junit.Test
//
//class BasicSchemasTest {
//
//  @Test def booleanFieldSchemaFor() = {
//    case class Testy(booly: Boolean)
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/boolean.json"))
//    val schema = AvroSchema[Testy]
//    assert(schema.toString(true) == expected.toString(true))
//  }
//
//  @Test def byteArrayFieldSchemaFor() = {
//    case class Testy(bytes: Array[Byte])
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bytes.json"))
//    val schema = AvroSchema[Testy]
//    assert(schema.toString(true) == expected.toString(true))
//  }
//
//  @Test def stringFieldSchemaFor() = {
//    case class Testy(str: String)
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/string.json"))
//    val schema = AvroSchema[Testy]
//    assert(schema.toString(true) == expected.toString(true))
//  }
//
//  @Test def intFieldSchemaFor() = {
//    case class Testy(inty: Int)
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/integer.json"))
//    val schema = AvroSchema[Testy]
//    assert(schema.toString(true) == expected.toString(true))
//  }
//
//  @Test def longFieldSchemaFor() = {
//    case class Testy(longy: Long)
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/long.json"))
//    val schema = AvroSchema[Testy]
//    assert(schema.toString(true) == expected.toString(true))
//  }
//
//  @Test def doubleFieldSchemaFor() = {
//    case class Testy(double: Double)
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/double.json"))
//    val schema = AvroSchema[Testy]
//    assert(schema.toString(true) == expected.toString(true))
//  }
//
//  @Test def floatFieldSchemaFor() = {
//    case class Testy(float: Float)
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/float.json"))
//    val schema = AvroSchema[Testy]
//    assert(schema.toString(true) == expected.toString(true))
//  }
//}
//
//case class Level4(str: Map[String, String])
//case class Level3(level4: Level4)
//case class Level2(level3: Level3)
//case class Level1(level2: Level2)