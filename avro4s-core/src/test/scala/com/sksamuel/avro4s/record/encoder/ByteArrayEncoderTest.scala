package com.sksamuel.avro4s.record.encoder

import java.nio.ByteBuffer

import com.sksamuel.avro4s.{AvroSchema, DefaultNamingStrategy, Encoder}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericFixed, GenericRecord}
import org.scalatest.{FunSuite, Matchers}

class ByteArrayEncoderTest extends FunSuite with Matchers {

  test("encode byte arrays as BYTES type") {
    case class Test(z: Array[Byte])
    val schema = AvroSchema[Test]
    Encoder[Test].encode(Test(Array[Byte](1, 4, 9)), schema)
      .asInstanceOf[GenericRecord]
      .get("z")
      .asInstanceOf[ByteBuffer]
      .array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("encode byte vectors as BYTES type") {
    case class Test(z: Vector[Byte])
    val schema = AvroSchema[Test]
    Encoder[Test].encode(Test(Vector[Byte](1, 4, 9)), schema)
      .asInstanceOf[GenericRecord]
      .get("z")
      .asInstanceOf[ByteBuffer]
      .array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("encode byte seq as BYTES type") {
    case class Test(z: Seq[Byte])
    val schema = AvroSchema[Test]
    Encoder[Test].encode(Test(Seq[Byte](1, 4, 9)), schema)
      .asInstanceOf[GenericRecord]
      .get("z")
      .asInstanceOf[ByteBuffer]
      .array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("encode byte list as BYTES type") {
    case class Test(z: List[Byte])
    val schema = AvroSchema[Test]
    Encoder[Test].encode(Test(List[Byte](1, 4, 9)), schema)
      .asInstanceOf[GenericRecord]
      .get("z")
      .asInstanceOf[ByteBuffer]
      .array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("encode top level byte arrays") {
    val schema = AvroSchema[Array[Byte]]
    Encoder[Array[Byte]].encode(Array[Byte](1, 4, 9), schema)
      .asInstanceOf[ByteBuffer]
      .array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("encode ByteBuffers as BYTES type") {
    case class Test(z: ByteBuffer)
    val schema = AvroSchema[Test]
    Encoder[Test].encode(Test(ByteBuffer.wrap(Array[Byte](1, 4, 9))), schema)
      .asInstanceOf[GenericRecord]
      .get("z")
      .asInstanceOf[ByteBuffer]
      .array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("encode top level ByteBuffers") {
    val schema = AvroSchema[ByteBuffer]
    Encoder[ByteBuffer].encode(ByteBuffer.wrap(Array[Byte](1, 4, 9)), schema)
      .asInstanceOf[ByteBuffer]
      .array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("support FIXED") {
    val schema = SchemaBuilder.fixed("foo").size(7)
    val fixed = Encoder.ByteArrayEncoder.encode("hello".getBytes, schema).asInstanceOf[GenericFixed]
    fixed.bytes().toList shouldBe Seq(104, 101, 108, 108, 111, 0, 0)
    fixed.bytes().length shouldBe 7
  }
}

