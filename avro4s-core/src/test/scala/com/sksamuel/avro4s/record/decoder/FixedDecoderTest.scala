package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroFixed, AvroSchema}
import com.sksamuel.avro4s.Decoder
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