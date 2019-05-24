package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.record.encoder.NamingTest
import com.sksamuel.avro4s.{Decoder, SchemaFor, SnakeCase}
import org.apache.avro.generic.GenericData
import org.scalatest.{FunSuite, Matchers}

class NamingStrategyDecoderTest extends FunSuite with Matchers {

  test("NamingStrategy should override the field names in a decoder") {
    val schema = SchemaFor[NamingTest].withNamingStrategy(SnakeCase)
    val decoder = Decoder[NamingTest].withNamingStrategy(SnakeCase)
    val record = new GenericData.Record(schema.schema)
    record.put("camel_case", "foo")
    val result = decoder.decode(record, schema.schema)
    result shouldBe NamingTest("foo")
  }

  test("NamingStrategy passed implicitly should override the field names in a decoder") {
    implicit val naming: SnakeCase.type = SnakeCase
    val schema = SchemaFor[NamingTest]
    val decoder = Decoder[NamingTest]
    val record = new GenericData.Record(schema.schema)
    record.put("camel_case", "foo")
    val result = decoder.decode(record, schema.schema)
    result shouldBe NamingTest("foo")
  }
}
