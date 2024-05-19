package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class StringDecoderTest extends AnyFunSuite with Matchers {

  test("decode from strings to strings") {
    val schema = AvroSchema[FooString]
    val record = GenericData.Record(schema)
    record.put("str", "hello")
    Decoder[FooString].decode(schema).apply(record) shouldBe FooString("hello")
  }

  test("decode from CharSequence to CharSequence") {
    case class CharSeqTest(val a: CharSequence)
    val schema = AvroSchema[CharSeqTest]
    val record = GenericData.Record(schema)
    record.put("a", "hello".subSequence(1, 2))
    Decoder[CharSeqTest].decode(schema).apply(record) shouldBe CharSeqTest("e")
  }

  test("decode from CharSequence to string") {
    case class CharSeqTest(val a: String)
    val schema = AvroSchema[CharSeqTest]
    val record = GenericData.Record(schema)
    record.put("a", "hello".subSequence(1, 2))
    Decoder[CharSeqTest].decode(schema).apply(record) shouldBe CharSeqTest("e")
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
    record.put("str", ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)))
    Decoder[FooString].decode(schema).apply(record) shouldBe FooString("hello")
  }

  test("decode from byte arrays to strings") {
    val schema = AvroSchema[FooString]
    val record = new GenericData.Record(schema)
    record.put("str", "hello".getBytes(StandardCharsets.UTF_8))
    Decoder[FooString].decode(schema).apply(record) shouldBe FooString("hello")
  }
}
