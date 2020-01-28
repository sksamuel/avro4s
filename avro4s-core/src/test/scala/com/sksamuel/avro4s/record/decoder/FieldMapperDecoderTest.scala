package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.record.encoder.NamingTest
import com.sksamuel.avro4s.{AvroSchemaV2, Decoder, FieldMapper, SchemaForV2, SnakeCase}
import org.apache.avro.generic.GenericData
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FieldMapperDecoderTest extends AnyFunSuite with Matchers {

  test("fieldMapper should override the field names in a decoder") {
    implicit val fieldMapper: FieldMapper = SnakeCase
    val schema = AvroSchemaV2[NamingTest]
    val decoder = Decoder[NamingTest]
    val record = new GenericData.Record(schema)
    record.put("camel_case", "foo")
    val result = decoder.decode(record)
    result shouldBe NamingTest("foo")
  }
}
