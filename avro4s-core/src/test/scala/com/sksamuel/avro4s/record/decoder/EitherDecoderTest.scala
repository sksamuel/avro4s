package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class EitherDecoderTest extends FunSuite with Matchers {

  case class Test(either: Either[String, Double])

  test("decode union:T,U for Either[T,U] of primitives") {
    Decoder[Test].decode(ImmutableRecord(AvroSchema[Test], Vector(new Utf8("foo")))) shouldBe Test(Left("foo"))
    Decoder[Test].decode(ImmutableRecord(AvroSchema[Test], Vector(java.lang.Double.valueOf(234.4D)))) shouldBe Test(Right(234.4D))
  }

  //  test("decode union:T,U for Either[T,U] of records") {
  //    case class Goo(s: String)
  //    case class Foo(b: Boolean)
  //    case class Test(either: Either[Goo, Foo])
  //    Decoder[Test].decode(Test(Left(Goo("zzz"))), AvroSchema[Test]) shouldBe ImmutableRecord(AvroSchema[Test], Vector(ImmutableRecord(AvroSchema[Goo], Vector(new Utf8("zzz")))))
  //    Decoder[Test].decode(Test(Right(Foo(true))), AvroSchema[Test]) shouldBe ImmutableRecord(AvroSchema[Test], Vector(ImmutableRecord(AvroSchema[Foo], Vector(java.lang.Boolean.valueOf(true)))))
  //  }
}

