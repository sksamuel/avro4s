package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, DefaultNamingStrategy, Encoder, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class NestedStructEncoderTest extends FunSuite with Matchers {

  test("encode nested structs") {

    case class Foo(s: String)
    case class Fooo(foo: Foo)
    case class Foooo(fooo: Fooo)

    Encoder[Foooo].encode(Foooo(Fooo(Foo("a"))), AvroSchema[Foooo], DefaultNamingStrategy) shouldBe
      ImmutableRecord(
        AvroSchema[Foooo],
        Vector(
          ImmutableRecord(
            AvroSchema[Fooo],
            Vector(
              ImmutableRecord(
                AvroSchema[Foo],
                Vector(new Utf8("a"))))
          )
        )
      )
  }
}