package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, AvroTransient, DecoderV2}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TransientDecoderTest extends AnyFunSuite with Matchers {

  case class TransientFoo(a: String, @AvroTransient b: Option[String])

  test("decoder should populate transient fields with None") {
    val schema = AvroSchema[TransientFoo]
    val record = new GenericData.Record(schema)
    record.put("a", new Utf8("hello"))
    DecoderV2[TransientFoo].decode(record) shouldBe TransientFoo("hello", None)
  }
}
