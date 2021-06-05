package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, Encoder, ImmutableRecord, SchemaFor}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericFixed, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StringEncoderTest extends AnyFunSuite with Matchers {

  test("encode strings as UTF8") {
    case class Foo(s: String)
    val schema = AvroSchema[Foo]
    val record = Encoder[Foo].encode(schema).apply(Foo("hello"))
    record shouldBe ImmutableRecord(schema, Vector(new Utf8("hello")))
  }

//  test("encode strings as GenericFixed and pad bytes when schema is fixed") {
//    case class Foo(s: String)
//
//    val fixedSchema = SchemaFor[String](Schema.createFixed("FixedString", null, null, 7))
//    implicit val fixedStringEncoder: Encoder[String] = Encoder.StringEncoder.withSchema(fixedSchema)
//
//    val record = Encoder[Foo].encode(Foo("hello")).asInstanceOf[GenericRecord]
//    record.get("s").asInstanceOf[GenericFixed].bytes().toList shouldBe Seq(104, 101, 108, 108, 111, 0, 0)
//    // the fixed should have the right size
//    record.get("s").asInstanceOf[GenericFixed].bytes().length shouldBe 7
//  }

}
