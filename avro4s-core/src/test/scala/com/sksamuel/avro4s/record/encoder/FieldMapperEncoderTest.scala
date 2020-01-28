package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{Encoder, SchemaForV2, SnakeCase}
import org.apache.avro.generic.GenericRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FieldMapperEncoderTest extends AnyFunSuite with Matchers {

  test("adding an in scope FieldMapper should overide the fields in an encoder") {
    implicit val fieldMapper = SnakeCase
    val schema: SchemaForV2[NamingTest] = SchemaForV2[NamingTest]
    val encoder = Encoder[NamingTest]
    val record = encoder.encode(NamingTest("Foo")).asInstanceOf[GenericRecord]
    record.get("camel_case")
  }

}

case class NamingTest(camelCase: String)