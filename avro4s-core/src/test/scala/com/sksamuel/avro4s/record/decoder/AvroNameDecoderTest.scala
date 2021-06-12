package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroName, AvroSchema, Decoder, DefaultFieldMapper, FieldMapper}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

case class Test123(@AvroName("bar") foo: String)

class AvroNameDecoderTest extends AnyFunSuite with Matchers {

  test("decoder should take into account @AvroName on fields") {
    val schema = AvroSchema[Test123]
    val record = new GenericData.Record(schema)
    record.put("bar", new Utf8("hello"))
    val decoder = Decoder[Test123]
    decoder.decode(schema).apply(record) shouldBe Test123("hello")
  }
}