package com.sksamuel.avro4s.schema

import java.nio.ByteBuffer

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ByteArraySchemaTest extends AnyFunSuite with Matchers {

  test("encode byte arrays as BYTES type") {
    case class Test(z: Array[Byte])
    val schema = AvroSchema[Test]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/byte_array.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("encode seq arrays as BYTES type") {
    case class Test(z: Seq[Byte])
    val schema = AvroSchema[Test]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/byte_array.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("encode list arrays as BYTES type") {
    case class Test(z: List[Byte])
    val schema = AvroSchema[Test]
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/byte_array.json"))
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("encode vector arrays as BYTES type") {
    case class Test(z: Vector[Byte])
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

