package com.sksamuel.avro4s.record.decoder

import java.nio.ByteBuffer

import com.sksamuel.avro4s.internal.{AvroSchema, Decoder}
import org.apache.avro.generic.GenericData
import org.scalatest.{FunSuite, Matchers}

class ByteArrayDecoderTest extends FunSuite with Matchers {

  case class Test(z: Array[Byte])

  test("decode byte arrays") {
    val schema = AvroSchema[Test]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[Test].decode(record).z.toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode top level byte arrays") {
    val schema = AvroSchema[Array[Byte]]
    Decoder[Array[Byte]].decode(ByteBuffer.wrap(Array[Byte](1, 4, 9))).toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode bytebuffers") {
    val schema = AvroSchema[Test]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[Test].decode(record).z.toList shouldBe List[Byte](1, 4, 9)
  }

  test("decode top level ByteBuffers") {
    val schema = AvroSchema[ByteBuffer]
    Decoder[ByteBuffer].decode(ByteBuffer.wrap(Array[Byte](1, 4, 9))).array().toList shouldBe List[Byte](1, 4, 9)
  }
}

