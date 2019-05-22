package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, AvroTransient, Decoder, DefaultNamingStrategy}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class TransientDecoderTest extends FunSuite with Matchers {

  case class TransientFoo(a: String, @AvroTransient b: Option[String])

  test("decoder should populate transient fields with None") {
    val schema = AvroSchema[TransientFoo]
    val record = new GenericData.Record(schema)
    record.put("a", new Utf8("hello"))
    Decoder[TransientFoo].decode(record, schema, DefaultNamingStrategy) shouldBe TransientFoo("hello", None)
  }
}
