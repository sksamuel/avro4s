package com.sksamuel.avro4s.streams.output

import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

class EitherOutputStreamTest extends OutputStreamTest {

  test("write out Lefts") {
    case class Test(z: Either[String, Int])
    writeRead(Test(Left("hello"))) { record =>
      record.get("z") shouldBe new Utf8("hello")
    }
  }

  test("write out Rights") {
    case class Test(z: Either[String, Int])
    writeRead(Test(Right(45))) { record =>
      record.get("z") shouldBe 45
    }
  }

  test("write out Lefts of case classes") {
    case class Foo(a: String)
    case class Bar(b: Boolean)
    case class Test(z: Either[Foo, Bar])
    writeRead(Test(Left(Foo("hello")))) { record =>
      record.get("z").asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("hello")
    }
  }

  test("write out rights of case classes") {
    case class Foo(a: String)
    case class Bar(b: Boolean)
    case class Test(z: Either[Foo, Bar])
    writeRead(Test(Right(Bar(true)))) { record =>
      record.get("z").asInstanceOf[GenericRecord].get("b") shouldBe true
    }
  }
}