package com.sksamuel.avro4s.record.decoder

//import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.sksamuel.avro4s._
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SchemaEvolutionTest extends AnyFunSuite with Matchers {

//  case class Version1(original: String)
//  case class Version2(@AvroAlias("original") renamed: String)

  case class P1(name: String, age: Int = 18)
  case class P2(name: String)

  case class OptionalStringTest(a: String, b: Option[String])
  case class DefaultStringTest(a: String, b: String = "foo")

//  ignore("@AvroAlias should be used when a reader schema has a field missing from the write schema") {
//
//    val v1schema = AvroSchema[Version1]
//    val v1 = Version1("hello")
//    val baos = new ByteArrayOutputStream()
//    val output = AvroOutputStream.data[Version1].to(baos).build()
//    output.write(v1)
//    output.close()
//
//    // we load using a v2 schema
//    val is = new AvroDataInputStream[Version2](new ByteArrayInputStream(baos.toByteArray), Some(v1schema))
//    val v2 = is.iterator.toList.head
//
//    v2.renamed shouldBe v1.original
//  }

  test("when decoding, if the record and schema are missing a field and the target has a scala default, use that") {
    val record = ToRecord[P2](AvroSchema[P2]).to(P2("foo"))
    FromRecord[P1](AvroSchema[P1]).from(record) shouldBe P1("foo", 18)
  }

  test("when decoding, if the record is missing a field that is present in the schema with a default, use the default from the schema") {
    val schema = SchemaBuilder.record("foo").fields().requiredString("a").endRecord()
    val record = new GenericData.Record(schema)
    record.put("a", new Utf8("hello"))
    FromRecord[DefaultStringTest](AvroSchema[DefaultStringTest]).from(record) shouldBe DefaultStringTest("hello")
  }

  test("when decoding, if the record is missing a field that is present in the schema and the type is option, then set to None") {
    val schema1 = SchemaBuilder.record("foo").fields().requiredString("a").endRecord()
    val record = new GenericData.Record(schema1)
    record.put("a", new Utf8("hello"))
    FromRecord[OptionalStringTest](AvroSchema[OptionalStringTest]).from(record) shouldBe OptionalStringTest("hello", None)
  }
}