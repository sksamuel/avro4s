package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.{FunSuite, Matchers}

class TransientSchemaTest extends FunSuite with Matchers {

  test("@transient fields should be ignored") {
    case class TransientFoo(a: String, @transient b: String, c: String)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/transient.json"))
    val schema = AvroSchema[TransientFoo]
    schema.toString(true) shouldBe expected.toString(true)
  }
}

