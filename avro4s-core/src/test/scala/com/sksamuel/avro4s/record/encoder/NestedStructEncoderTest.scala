package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, AvroSchemaV2, Encoder, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class NestedStructEncoderTest extends AnyFunSuite with Matchers {

  test("encode nested structs") {

    case class Foo(s: String)
    case class Fooo(foo: Foo)
    case class Foooo(fooo: Fooo)

    Encoder[Foooo].encode(Foooo(Fooo(Foo("a")))) shouldBe
      ImmutableRecord(
        AvroSchemaV2[Foooo],
        Vector(
          ImmutableRecord(
            AvroSchemaV2[Fooo],
            Vector(
              ImmutableRecord(
                AvroSchemaV2[Foo],
                Vector(new Utf8("a"))))
          )
        )
      )
  }
}