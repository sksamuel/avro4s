package com.sksamuel.avro4s.record

import com.sksamuel.avro4s.{AvroNamespace, AvroSchema, ToRecord}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

sealed trait Foo
case class Bar(i: Int) extends Foo
case class Baz(s: String) extends Foo

case class MySchema(@AvroNamespace("broken") foo: Foo, id: String, x: Int)

class ToRecordTest extends AnyFunSuite with Matchers {

  test("ToRecord should work with a namespace annotation on an ADT") {
    val schema = AvroSchema[MySchema]

    val ms = MySchema(Bar(1), "", 0)
    ToRecord[MySchema](schema).to(ms) //throws
  }
}
