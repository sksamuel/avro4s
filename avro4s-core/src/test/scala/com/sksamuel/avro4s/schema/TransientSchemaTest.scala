package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema, AvroTransient}
import org.scalatest.{FunSuite, Matchers}

class TransientSchemaTest extends FunSuite with Matchers {

  test("@AvroTransient fields should be ignored") {
    case class TransientFoo(a: String, @AvroTransient b: String, c: String)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/transient.json"))
    val schema = AvroSchema[TransientFoo]
    schema.toString(true) shouldBe expected.toString(true)
  }
}