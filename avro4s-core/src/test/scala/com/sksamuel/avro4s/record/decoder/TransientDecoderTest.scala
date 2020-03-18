package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, AvroSchemaV2, AvroTransient, Decoder}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TransientDecoderTest extends AnyFunSuite with Matchers {

  case class TransientFoo(a: String, @AvroTransient b: Option[String])

  test("decoder should populate transient fields with None") {
    val schema = AvroSchemaV2[TransientFoo]
    val record = new GenericData.Record(schema)
    record.put("a", new Utf8("hello"))
    Decoder[TransientFoo].decode(record) shouldBe TransientFoo("hello", None)
  }
}
