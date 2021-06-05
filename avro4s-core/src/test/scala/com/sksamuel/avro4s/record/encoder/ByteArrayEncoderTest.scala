package com.sksamuel.avro4s.record.encoder

import java.nio.ByteBuffer
import com.sksamuel.avro4s.{AvroSchema, Encoder, SchemaFor}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericFixed, GenericRecord}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ByteArrayEncoderTest extends AnyFunSuite with Matchers {

  test("encode byte arrays as BYTES type") {
    case class Test(z: Array[Byte])
    val schema = AvroSchema[Test]
    Encoder[Test].encode(schema)
      .apply(Test(Array[Byte](1, 4, 9)))
      .asInstanceOf[GenericRecord]
      .get("z")
      .asInstanceOf[ByteBuffer]
      .array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("encode byte vectors as BYTES type") {
    case class Test(z: Vector[Byte])
    val schema = AvroSchema[Test]
    Encoder[Test]
      .encode(schema)
      .apply(Test(Vector[Byte](1, 4, 9)))
      .asInstanceOf[GenericRecord]
      .get("z")
      .asInstanceOf[ByteBuffer]
      .array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("encode byte seq as BYTES type") {
    case class Test(z: Seq[Byte])
    val schema = AvroSchema[Test]
    Encoder[Test]
      .encode(schema)
      .apply(Test(Seq[Byte](1, 4, 9)))
      .asInstanceOf[GenericRecord]
      .get("z")
      .asInstanceOf[ByteBuffer]
      .array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("encode byte list as BYTES type") {
    case class Test(z: List[Byte])
    val schema = AvroSchema[Test]
    Encoder[Test]
      .encode(schema)
      .apply(Test(List[Byte](1, 4, 9)))
      .asInstanceOf[GenericRecord]
      .get("z")
      .asInstanceOf[ByteBuffer]
      .array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("encode ByteBuffers as BYTES type") {
    case class Test(z: ByteBuffer)
    val schema = AvroSchema[Test]
    Encoder[Test]
      .encode(schema)
      .apply(Test(ByteBuffer.wrap(Array[Byte](1, 4, 9))))
      .asInstanceOf[GenericRecord]
      .get("z")
      .asInstanceOf[ByteBuffer]
      .array().toList shouldBe List[Byte](1, 4, 9)
  }

//  test("encode top level byte arrays") {
//    val encoder = Encoder[Array[Byte]].resolveEncoder()
//    encoder.schema shouldBe SchemaBuilder.builder().bytesType()
//    encoder.encode(Array[Byte](1, 4, 9))
//      .asInstanceOf[ByteBuffer]
//      .array().toList shouldBe List[Byte](1, 4, 9)
//  }

//  test("encode top level ByteBuffers") {
//    Encoder[ByteBuffer]
//      .apply(ByteBuffer.wrap(Array[Byte](1, 4, 9)))
//      .asInstanceOf[ByteBuffer]
//      .array().toList shouldBe List[Byte](1, 4, 9)
//  }

//  test("support FIXED") {
//    val schema = SchemaBuilder.fixed("foo").size(7)
//    val fixed = Encoder.ByteArrayEncoder.withSchema(SchemaFor(schema)).encode("hello".getBytes).asInstanceOf[GenericFixed]
//    fixed.bytes().toList shouldBe Seq(104, 101, 108, 108, 111, 0, 0)
//    fixed.bytes().length shouldBe 7
//  }
}

