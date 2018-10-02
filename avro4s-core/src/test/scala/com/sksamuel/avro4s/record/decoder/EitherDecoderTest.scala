package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

case class Test(either: Either[String, Double])
case class Goo(s: String)
case class Foo(b: Boolean)
case class Test2(either: Either[Goo, Foo])

class EitherDecoderTest extends FunSuite with Matchers {

  case class Voo(s: String)
  case class Woo(b: Boolean)
  case class Test3(either: Either[Voo, Woo])

  test("decode union:T,U for Either[T,U] of primitives") {
    Decoder[Test].decode(ImmutableRecord(AvroSchema[Test], Vector(new Utf8("foo")))) shouldBe Test(Left("foo"))
    Decoder[Test].decode(ImmutableRecord(AvroSchema[Test], Vector(java.lang.Double.valueOf(234.4D)))) shouldBe Test(Right(234.4D))
  }

  test("decode union:T,U for Either[T,U] of top level classes") {
    Decoder[Test2].decode(ImmutableRecord(AvroSchema[Test2], Vector(ImmutableRecord(AvroSchema[Goo], Vector(new Utf8("zzz")))))) shouldBe Test2(Left(Goo("zzz")))
    Decoder[Test2].decode(ImmutableRecord(AvroSchema[Test2], Vector(ImmutableRecord(AvroSchema[Foo], Vector(java.lang.Boolean.valueOf(true)))))) shouldBe Test2(Right(Foo(true)))
  }

  test("decode union:T,U for Either[T,U] of nested classes") {
    Decoder[Test3].decode(ImmutableRecord(AvroSchema[Test3], Vector(ImmutableRecord(AvroSchema[Voo], Vector(new Utf8("zzz")))))) shouldBe Test3(Left(Voo("zzz")))
    Decoder[Test3].decode(ImmutableRecord(AvroSchema[Test3], Vector(ImmutableRecord(AvroSchema[Woo], Vector(java.lang.Boolean.valueOf(true)))))) shouldBe Test3(Right(Woo(true)))
  }
}

