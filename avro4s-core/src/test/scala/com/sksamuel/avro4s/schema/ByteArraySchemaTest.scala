package com.sksamuel.avro4s.schema

import java.nio.ByteBuffer

import com.sksamuel.avro4s.internal.AvroSchema
import org.scalatest.{FunSuite, Matchers}

class ByteArraySchemaTest extends FunSuite with Matchers {

  test("encode byte arrays as BYTES type") {
    case class Test(z: Array[Byte])
    val schema = AvroSchema[Test]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/byte_array.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support top level byte arrays") {
    val schema = AvroSchema[Array[Byte]]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_byte_array.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("encode ByteBuffer as BYTES type") {
    case class Test(z: ByteBuffer)
    val schema = AvroSchema[Test]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bytebuffer.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support top level ByteBuffers") {
    val schema = AvroSchema[ByteBuffer]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_bytebuffer.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }
}

