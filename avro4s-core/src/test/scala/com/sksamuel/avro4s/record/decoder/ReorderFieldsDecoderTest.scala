//package com.sksamuel.avro4s.record.decoder
//
//import com.sksamuel.avro4s.{AvroSchema, Decoder, Encoder, SchemaFor}
//import com.sksamuel.avro4s.record.decoder.ReorderFieldsDecoderTest.TestClass
//import org.apache.avro.Schema
//import org.apache.avro.generic.GenericData
//import org.scalatest.matchers.must.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//
//class ReorderFieldsDecoderTest extends AnyWordSpec with Matchers {
//
//  val schema = new Schema.Parser().parse(
//    """
//      |{
//      |  "type" : "record",
//      |  "name" : "TestClass",
//      |  "namespace" : "com.sksamuel.avro4s.record.decoder.ReorderFieldsDecoderTest",
//      |  "fields" : [ {
//      |    "name" : "second",
//      |    "type" : "int"
//      |  }, {
//      |    "name" : "first",
//      |    "type" : "string"
//      |  } ]
//      |}""".stripMargin)
//
//  "RecordDecoder" should {
//    "respect decoding order" in {
//      val decoder = Decoder[TestClass].withSchema(SchemaFor(schema))
//      val record = new GenericData.Record(schema)
//      record.put("second", 42)
//      record.put("first", "hello")
//      decoder.decode(record) mustBe TestClass("hello", 42)
//    }
//  }
//}
//
//object ReorderFieldsDecoderTest {
//  case class TestClass(first: String, second: Int)
//}
