package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, AvroTransient, DefaultNamingStrategy, Encoder, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class AvroTransientEncoderTest extends FunSuite with Matchers {

  test("encoder should skip @AvroTransient fields") {
    case class Foo(a: String, @AvroTransient b: String, c: String)
    val record = Encoder[Foo].encode(Foo("a", "b", "c"), AvroSchema[Foo]).asInstanceOf[ImmutableRecord]
    record.values shouldBe Vector(new Utf8("a"), new Utf8("c"))
  }
}
