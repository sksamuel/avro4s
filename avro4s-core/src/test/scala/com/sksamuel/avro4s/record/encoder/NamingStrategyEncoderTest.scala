package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{Encoder, SchemaFor, SnakeCase}
import org.apache.avro.generic.GenericRecord
import org.scalatest.{FunSuite, Matchers}

class NamingStrategyEncoderTest extends FunSuite with Matchers {

  test("adding an in scope NamingStrategy should overide the fields in an encoder") {
    val schema = SchemaFor[NamingTest].schema(SnakeCase)
    val encoder = Encoder[NamingTest]
    val record = encoder.encode(NamingTest("Foo"), schema, SnakeCase).asInstanceOf[GenericRecord]
    record.get("camel_case")
  }

}

case class NamingTest(camelCase: String)