package com.sksamuel.avro4s

import org.scalatest.{FunSuite, Matchers}

sealed trait Foo
case class Bar(i: Int) extends Foo
case class Baz(s: String) extends Foo

case class MySchema(@AvroNamespace("broken") foo: Foo, id: String, x: Int)

class ToRecordTest extends FunSuite with Matchers {

  ignore("ToRecord should work with a namespace annotation on an ADT") {
    val schema = AvroSchema[MySchema]

    val ms = MySchema(Bar(1), "", 0)
    ToRecord[MySchema](schema).to(ms) //throws
  }
}
