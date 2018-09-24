package com.sksamuel.avro4s.streams.output

import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

class EitherDataOutputStreamTest extends DataOutputStreamTest {

  test("write out Lefts") {
    case class Test(z: Either[String, Int])
    val out = write(Test(Left("hello")))
    val record = read[Test](out)
    record.get("z") shouldBe new Utf8("hello")
  }

  test("write out Rights") {
    case class Test(z: Either[String, Int])
    val out = write(Test(Right(45)))
    val record = read[Test](out)
    record.get("z") shouldBe 45
  }

  test("write out Lefts of case classes") {
    case class Foo(a: String)
    case class Bar(b: Boolean)
    case class Test(z: Either[Foo, Bar])
    val out = write(Test(Left(Foo("hello"))))
    val record = read[Test](out)
    record.get("z").asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("hello")
  }

  test("write out rights of case classes") {
    case class Foo(a: String)
    case class Bar(b: Boolean)
    case class Test(z: Either[Foo, Bar])
    val out = write(Test(Right(Bar(true))))
    val record = read[Test](out)
    record.get("z").asInstanceOf[GenericRecord].get("b") shouldBe true
  }
}