package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.record.decoder.ReorderFieldsDecoderTest.TestClass
import com.sksamuel.avro4s.{Decoder, Encoder, ImmutableRecord, SchemaFor}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ReorderFieldsEncoderTest extends AnyWordSpec with Matchers {

  val schema = new Schema.Parser().parse("""
      |{
      |  "type" : "record",
      |  "name" : "TestClass",
      |  "namespace" : "com.sksamuel.avro4s.record.decoder.ReorderFieldsDecoderTest",
      |  "fields" : [ {
      |    "name" : "second",
      |    "type" : "int"
      |  }, {
      |    "name" : "first",
      |    "type" : "string"
      |  } ]
      |}""".stripMargin)

  "RecordDecoder" should {
    "respect decoding order" in {
      val encoder = Encoder[TestClass].withSchema(SchemaFor(schema))
      encoder.encode(TestClass("hello", 42)) mustBe ImmutableRecord(schema, Seq(Integer.valueOf(42), new Utf8("hello")))
    }
  }
}

object ReorderFieldsEncoderTest {
  case class TestClass(first: String, second: Int)
}
