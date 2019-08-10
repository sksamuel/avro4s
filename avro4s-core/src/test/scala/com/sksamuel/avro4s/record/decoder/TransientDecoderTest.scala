package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, AvroTransient, Decoder, DefaultFieldMapper}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class TransientDecoderTest extends FunSuite with Matchers {

  case class TransientFoo(a: String, @AvroTransient b: Option[String])

  test("decoder should populate transient fields with None") {
    val schema = AvroSchema[TransientFoo]
    val record = new GenericData.Record(schema)
    record.put("a", new Utf8("hello"))
    Decoder[TransientFoo].decode(record, schema, DefaultFieldMapper) shouldBe TransientFoo("hello", None)
  }
}
