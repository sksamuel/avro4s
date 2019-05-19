package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.record.encoder.NamingTest
import com.sksamuel.avro4s.{AvroSchema, Decoder, SnakeCase}
import org.apache.avro.generic.GenericData
import org.scalatest.{FunSuite, Matchers}

class NamingStrategyDecoderTest extends FunSuite with Matchers {

  test("adding an in scope NamingStrategy should overide the field names in a decoder") {
    implicit val naming = SnakeCase
    val schema = AvroSchema[NamingTest]
    val decoder = Decoder[NamingTest]
    val record = new GenericData.Record(schema)
    record.put("camel_case", "foo")
    val result = decoder.decode(record, schema)
    result shouldBe NamingTest("foo")
  }
}
