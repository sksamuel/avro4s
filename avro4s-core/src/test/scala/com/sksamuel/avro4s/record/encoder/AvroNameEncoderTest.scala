package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroName, AvroSchema, Encoder}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class AvroNameEncoderTest extends FunSuite with Matchers {

  case class AvroNameEncoderTest(@AvroName("bar") foo: String)

  test("encoder should take into account @AvroName") {
    val schema = AvroSchema[AvroNameEncoderTest]
    val record = Encoder[AvroNameEncoderTest].encode(AvroNameEncoderTest("hello"), schema).asInstanceOf[GenericRecord]
    record.get("bar") shouldBe new Utf8("hello")
  }
}