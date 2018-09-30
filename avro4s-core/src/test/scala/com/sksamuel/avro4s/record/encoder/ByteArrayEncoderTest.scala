package com.sksamuel.avro4s.record.encoder

import java.nio.ByteBuffer

import com.sksamuel.avro4s.internal.{AvroSchema, Encoder}
import org.apache.avro.generic.GenericRecord
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
}

