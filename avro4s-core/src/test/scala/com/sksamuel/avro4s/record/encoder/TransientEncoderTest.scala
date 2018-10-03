package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, Encoder, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class TransientEncoderTest extends FunSuite with Matchers {

  test("encoder should skip transient fields") {
    case class Foo(a: String, @transient b: String, c: String)
    val record = Encoder[Foo].encode(Foo("a", "b", "c"), AvroSchema[Foo]).asInstanceOf[ImmutableRecord]
    record.values shouldBe Vector(new Utf8("a"), new Utf8("c"))
  }
}
