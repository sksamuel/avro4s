package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, AvroTransient, Decoder}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TransientDecoderTest extends AnyFunSuite with Matchers {

  case class TransientFoo(a: String, @AvroTransient b: Option[String])

  test("decoder should populate transient fields with None") {
    val schema = AvroSchema[TransientFoo]
    val record = new GenericData.Record(schema)
    record.put("a", new Utf8("hello"))
    Decoder[TransientFoo].decode(schema).apply(record) shouldBe TransientFoo("hello", None)
  }

  case class Foo(name:String)
  case class TransientFooWithDefault(a: String, @AvroTransient(true) b: Foo = Foo("hello"))

  test("decoder should populate transient fields with default case class value") {
    val schema = AvroSchema[TransientFooWithDefault]
    val record = new GenericData.Record(schema)
    record.put("a", new Utf8("hello"))
    Decoder[TransientFooWithDefault].decode(schema).apply(record) shouldBe TransientFooWithDefault("hello")
  }
}
