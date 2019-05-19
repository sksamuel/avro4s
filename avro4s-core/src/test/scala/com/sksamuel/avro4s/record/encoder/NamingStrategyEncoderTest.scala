package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, Encoder, SnakeCase}
import org.apache.avro.generic.GenericRecord
import org.scalatest.{FunSuite, Matchers}

class NamingStrategyEncoderTest extends FunSuite with Matchers {

  test("adding an in scope NamingStrategy should overide the fields in an encoder") {
    implicit val naming = SnakeCase
    val schema = AvroSchema[NamingTest]
    val encoder = Encoder[NamingTest]
    val record = encoder.encode(NamingTest("Foo"), schema).asInstanceOf[GenericRecord]
    record.get("camel_case")
  }

}

case class NamingTest(camelCase: String)