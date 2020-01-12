package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroName, AvroSchema, AvroSchemaV2, Decoder, DecoderV2, DefaultFieldMapper, FieldMapper}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AvroNameDecoderTest extends AnyFunSuite with Matchers {

  case class AvroNameDecoderTest(@AvroName("bar") foo: String)
  implicit val fm: FieldMapper = DefaultFieldMapper

  test("decoder should take into account @AvroName on fields") {
    val schema = AvroSchemaV2[AvroNameDecoderTest]
    val record = new GenericData.Record(schema)
    record.put("bar", new Utf8("hello"))
    DecoderV2[AvroNameDecoderTest].decode(record) shouldBe AvroNameDecoderTest("hello")
  }
}