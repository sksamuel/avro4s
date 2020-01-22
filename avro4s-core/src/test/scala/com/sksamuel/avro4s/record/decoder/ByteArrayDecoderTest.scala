package com.sksamuel.avro4s.record.decoder

import java.nio.ByteBuffer

import com.sksamuel.avro4s.{AvroSchemaV2, DecoderV2}
import org.apache.avro.generic.GenericData
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.higherKinds

class ByteArrayDecoderTest extends AnyFunSuite with Matchers {

  case class ArrayTest(z: Array[Byte])
  case class ByteBufferTest(z: ByteBuffer)
  case class VectorTest(z: Vector[Byte])
  case class SeqTest(z: Array[Byte])
  case class ListTest(z: Array[Byte])

  test("decode byte arrays") {
    val schema = AvroSchemaV2[ArrayTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    DecoderV2[ArrayTest].decode(record).z.toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode bytebuffers to array") {
    val schema = AvroSchemaV2[ArrayTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    DecoderV2[ArrayTest].decode(record).z.toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode byte vectors") {
    val schema = AvroSchemaV2[VectorTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    DecoderV2[VectorTest].decode(record).z shouldBe Vector[Byte](1, 4, 9)
  }

  test("decode byte lists") {
    val schema = AvroSchemaV2[ListTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    DecoderV2[ListTest].decode(record).z shouldBe List[Byte](1, 4, 9)
  }

  test("decode byte seqs") {
    val schema = AvroSchemaV2[SeqTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    DecoderV2[SeqTest].decode(record).z shouldBe Seq[Byte](1, 4, 9)
  }

  test("decode top level byte arrays") {
    DecoderV2[Array[Byte]].decode(ByteBuffer.wrap(Array[Byte](1, 4, 9))).toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode array to bytebuffers") {
    val schema = AvroSchemaV2[ByteBufferTest]
    val record = new GenericData.Record(schema)
    record.put("z", Array[Byte](1, 4, 9))
    DecoderV2[ByteBufferTest].decode(record).z.array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode bytebuffers") {
    val schema = AvroSchemaV2[ByteBufferTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    DecoderV2[ByteBufferTest].decode(record).z.array().toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode top level ByteBuffers") {
    DecoderV2[ByteBuffer].decode(ByteBuffer.wrap(Array[Byte](1, 4, 9))).array().toList shouldBe List[Byte](1, 4, 9)
  }
}

