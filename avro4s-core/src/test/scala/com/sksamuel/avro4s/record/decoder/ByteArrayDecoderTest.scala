package com.sksamuel.avro4s.record.decoder

import java.nio.ByteBuffer

import com.sksamuel.avro4s.internal.{AvroSchema, Decoder}
import org.apache.avro.generic.GenericData
import org.scalatest.{FunSuite, Matchers}

class ByteArrayDecoderTest extends FunSuite with Matchers {

  case class Test(z: Array[Byte])

  test("decode byte arrays as BYTES type") {
    val schema = AvroSchema[Test]

    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))

    Decoder[Test].decode(record).z.toList shouldBe List[Byte](1, 4, 9)
  }
}

