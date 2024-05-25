package com.sksamuel.avro4s.record.decoder

import java.nio.ByteBuffer

import com.sksamuel.avro4s.{AvroSchema, Decoder}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ByteArrayDecoderTest extends AnyFunSuite with Matchers {

  case class ArrayTest(z: Array[Byte])
  case class ByteBufferTest(z: ByteBuffer)
  case class VectorTest(z: Vector[Byte])
  case class SeqTest(z: Seq[Byte])
  case class ListTest(z: List[Byte])

  test("decode Array[Byte] to Array[Byte]") {
    val schema = AvroSchema[ArrayTest]
    val record = new GenericData.Record(schema)
    record.put("z", Array[Byte](1, 4, 9))
    Decoder[ArrayTest].decode(schema).apply(record).z.toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode ByteBuffer to Array[Byte]") {
    val schema = AvroSchema[ArrayTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[ArrayTest].decode(schema).apply(record).z.toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode Array[Byte] to Vector[Byte]") {
    val schema = AvroSchema[VectorTest]
    val record = new GenericData.Record(schema)
    record.put("z", Array[Byte](1, 4, 9))
    Decoder[ArrayTest].decode(schema).apply(record).z.toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode ByteBuffer to Vector[Byte]") {
    val schema = AvroSchema[VectorTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[VectorTest].decode(schema).apply(record).z shouldBe Vector[Byte](1, 4, 9)
  }

  test("decode read-only ByteBuffer to Vector[Byte]") {
    val schema = AvroSchema[VectorTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)).asReadOnlyBuffer())
    Decoder[VectorTest].decode(schema).apply(record).z shouldBe Vector[Byte](1, 4, 9)
  }

  test("decode Array[Byte] to List[Byte]") {
    val schema = AvroSchema[ListTest]
    val record = new GenericData.Record(schema)
    record.put("z", Array[Byte](1, 4, 9))
    Decoder[ArrayTest].decode(schema).apply(record).z.toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode ByteBuffer to List[Byte]") {
    val schema = AvroSchema[ListTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[ListTest].decode(schema).apply(record).z shouldBe List[Byte](1, 4, 9)
  }

  test("decode Array[Byte] to Seq[Byte]") {
    val schema = AvroSchema[SeqTest]
    val record = new GenericData.Record(schema)
    record.put("z", Array[Byte](1, 4, 9))
    Decoder[ArrayTest].decode(schema).apply(record).z.toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode ByteBuffer to Seq[Byte]") {
    val schema = AvroSchema[SeqTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[SeqTest].decode(schema).apply(record).z shouldBe Seq[Byte](1, 4, 9)
  }

  test("decode top level byte arrays") {
    val schema = AvroSchema[Array[Byte]]
    val decoder = Decoder[Array[Byte]]
    decoder.decode(schema).apply(ByteBuffer.wrap(Array[Byte](1, 4, 9))).toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode Array[Byte] to ByteBuffer") {
    val schema = AvroSchema[ByteBufferTest]
    val record = new GenericData.Record(schema)
    record.put("z", Array[Byte](1, 4, 9))
    Decoder[ByteBufferTest].decode(schema).apply(record).z.array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode bytebuffers") {
    val schema = AvroSchema[ByteBufferTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[ByteBufferTest].decode(schema).apply(record).z.array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode ByteBuffer to top level ByteBuffer") {
    val schema = AvroSchema[ByteBuffer]
    Decoder[ByteBuffer].decode(schema).apply(ByteBuffer.wrap(Array[Byte](1, 4, 9))).array().toList shouldBe List[Byte](1, 4, 9)
  }
}

