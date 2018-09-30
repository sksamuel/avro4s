package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.AvroFixed
import com.sksamuel.avro4s.internal.{AvroSchema, Decoder}
import org.apache.avro.generic.GenericData
import org.scalatest.{FunSuite, Matchers}

class FixedDecoderTest extends FunSuite with Matchers {

  case class WithFixedString(@AvroFixed(10) z: String)

  test("decode bytes to String") {
    val schema = AvroSchema[WithFixedString]
    val record = new GenericData.Record(schema)
    record.put("z", Array[Byte](115, 97, 109))
    Decoder[WithFixedString].decode(record) shouldBe WithFixedString("sam")
  }
}