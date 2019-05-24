package com.sksamuel.avro4s.record.decoder

import java.nio.ByteBuffer

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultNamingStrategy}
import org.apache.avro.generic.GenericData
import org.scalatest.{FunSuite, Matchers}

import scala.language.higherKinds

class ByteArrayDecoderTest extends FunSuite with Matchers {

  case class ArrayTest(z: Array[Byte])
  case class ByteBufferTest(z: ByteBuffer)
  case class VectorTest(z: Vector[Byte])
  case class SeqTest(z: Array[Byte])
  case class ListTest(z: Array[Byte])

  test("decode byte arrays") {
    val schema = AvroSchema[ArrayTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[ArrayTest].decode(record, schema).z.toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode bytebuffers to array") {
    val schema = AvroSchema[ArrayTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[ArrayTest].decode(record, schema).z.toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode byte vectors") {
    val schema = AvroSchema[VectorTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[VectorTest].decode(record, schema).z shouldBe Vector[Byte](1, 4, 9)
  }

  test("decode byte lists") {
    val schema = AvroSchema[ListTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[ListTest].decode(record, schema).z shouldBe List[Byte](1, 4, 9)
  }

  test("decode byte seqs") {
    val schema = AvroSchema[SeqTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[SeqTest].decode(record, schema).z shouldBe Seq[Byte](1, 4, 9)
  }

  test("decode top level byte arrays") {
    Decoder[Array[Byte]].decode(ByteBuffer.wrap(Array[Byte](1, 4, 9)), AvroSchema[Array[Byte]]).toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode array to bytebuffers") {
    val schema = AvroSchema[ByteBufferTest]
    val record = new GenericData.Record(schema)
    record.put("z", Array[Byte](1, 4, 9))
    Decoder[ByteBufferTest].decode(record, schema).z.array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode bytebuffers") {
    val schema = AvroSchema[ByteBufferTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[ByteBufferTest].decode(record, schema).z.array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode top level ByteBuffers") {
    Decoder[ByteBuffer].decode(ByteBuffer.wrap(Array[Byte](1, 4, 9)), AvroSchema[ByteBuffer]).array().toList shouldBe List[Byte](1, 4, 9)
  }
}

