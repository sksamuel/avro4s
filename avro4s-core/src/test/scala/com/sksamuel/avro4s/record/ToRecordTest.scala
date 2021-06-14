package com.sksamuel.avro4s.record

import com.sksamuel.avro4s.{AvroNamespace, AvroSchema, FromRecord, ToRecord}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

sealed trait Foo
case class Bar(i: Int) extends Foo
case class Baz(s: String) extends Foo

case class MySchema(@AvroNamespace("broken") foo: Foo, id: String, x: Int)

class ToRecordTest extends AnyFunSuite with Matchers {

  test("encode to record") {
    val schema = AvroSchema[HasSomeFields]
    val record = ToRecord[HasSomeFields](schema).to(HasSomeFields("hello", 42, false, Nested("there")))
    record.get("str") shouldBe new Utf8("hello")
    record.get("int").asInstanceOf[Int] shouldBe 42
    record.get("boolean").asInstanceOf[Boolean] shouldBe false
    record.get("nested").asInstanceOf[GenericRecord].get("foo") shouldBe new Utf8("there")
  }

  //  test("ToRecord should work with a namespace annotation on an ADT") {
  //    val schema = AvroSchema[MySchema]
  //    val ms = MySchema(Bar(1), "", 0)
  //    ToRecord[MySchema](schema).to(ms)
  //  }
}
