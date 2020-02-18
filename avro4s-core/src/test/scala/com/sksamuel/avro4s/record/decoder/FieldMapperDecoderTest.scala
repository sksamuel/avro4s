package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.record.encoder.NamingTest
import com.sksamuel.avro4s.{Decoder, SchemaFor, SnakeCase}
import org.apache.avro.generic.GenericData
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FieldMapperDecoderTest extends AnyFunSuite with Matchers {

  test("fieldMapper should overide the field names in a decoder") {
    val schema = SchemaFor[NamingTest].schema(SnakeCase)
    val decoder = Decoder[NamingTest]
    val record = new GenericData.Record(schema)
    record.put("camel_case", "foo")
    val result = decoder.decode(record, schema, SnakeCase)
    result shouldBe NamingTest("foo")
  }
}
