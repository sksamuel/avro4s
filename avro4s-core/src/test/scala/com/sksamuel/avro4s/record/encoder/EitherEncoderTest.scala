package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, Encoder, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EitherEncoderTest extends AnyFunSuite with Matchers {

  test("generate union:T,U for Either[T,U] of primitives") {
    case class Test(either: Either[String, Double])
    val schema = AvroSchema[Test]
    Encoder[Test].encode(schema).apply(Test(Left("foo"))) shouldBe ImmutableRecord(AvroSchema[Test], Vector(new Utf8("foo")))
    Encoder[Test].encode(schema).apply(Test(Right(234.4D))) shouldBe ImmutableRecord(AvroSchema[Test], Vector(java.lang.Double.valueOf(234.4D)))
  }

  test("generate union:T,U for Either[T,U] of case classes") {
    case class Goo(s: String)
    case class Foo(b: Boolean)
    case class Test(either: Either[Goo, Foo])
    val schema = AvroSchema[Test]
    Encoder[Test].encode(schema).apply(Test(Left(Goo("zzz")))) shouldBe ImmutableRecord(AvroSchema[Test], Vector(ImmutableRecord(AvroSchema[Goo], Vector(new Utf8("zzz")))))
    Encoder[Test].encode(schema).apply(Test(Right(Foo(true)))) shouldBe ImmutableRecord(AvroSchema[Test], Vector(ImmutableRecord(AvroSchema[Foo], Vector(java.lang.Boolean.valueOf(true)))))
  }
}

