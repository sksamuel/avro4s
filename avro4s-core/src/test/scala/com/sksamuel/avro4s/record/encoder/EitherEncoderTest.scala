package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchemaV2, Encoder, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EitherEncoderTest extends AnyFunSuite with Matchers {

  test("generate union:T,U for Either[T,U] of primitives") {
    case class Test(either: Either[String, Double])
    Encoder[Test].encode(Test(Left("foo"))) shouldBe ImmutableRecord(AvroSchemaV2[Test], Vector(new Utf8("foo")))
    Encoder[Test].encode(Test(Right(234.4D))) shouldBe ImmutableRecord(AvroSchemaV2[Test], Vector(java.lang.Double.valueOf(234.4D)))
  }

  test("generate union:T,U for Either[T,U] of records") {
    case class Goo(s: String)
    case class Foo(b: Boolean)
    case class Test(either: Either[Goo, Foo])
    Encoder[Test].encode(Test(Left(Goo("zzz")))) shouldBe ImmutableRecord(AvroSchemaV2[Test], Vector(ImmutableRecord(AvroSchemaV2[Goo], Vector(new Utf8("zzz")))))
    Encoder[Test].encode(Test(Right(Foo(true)))) shouldBe ImmutableRecord(AvroSchemaV2[Test], Vector(ImmutableRecord(AvroSchemaV2[Foo], Vector(java.lang.Boolean.valueOf(true)))))
  }
}

