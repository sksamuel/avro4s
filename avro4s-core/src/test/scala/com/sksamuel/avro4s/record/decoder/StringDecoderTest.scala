package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer

class StringDecoderTest extends AnyFunSuite with Matchers {

  test("decode from strings to strings") {
    val schema = AvroSchema[FooString]
    val record = GenericData.Record(schema)
    record.put("str", "hello")
    Decoder[FooString].decode(schema).apply(record) shouldBe FooString("hello")
  }

  test("decode from UTF8s to strings") {
    val schema = AvroSchema[FooString]
    val record = new GenericData.Record(schema)
    record.put("str", Utf8("hello"))
    Decoder[FooString].decode(schema).apply(record) shouldBe FooString("hello")
  }

  test("decode from byte buffers to strings") {
    val schema = AvroSchema[FooString]
    val record = new GenericData.Record(schema)
    record.put("str", ByteBuffer.wrap("hello".getBytes))
    Decoder[FooString].decode(schema).apply(record) shouldBe FooString("hello")
  }

  test("decode from byte arrays to strings") {
    val schema = AvroSchema[FooString]
    val record = new GenericData.Record(schema)
    record.put("str", "hello".getBytes)
    Decoder[FooString].decode(schema).apply(record) shouldBe FooString("hello")
  }
}
