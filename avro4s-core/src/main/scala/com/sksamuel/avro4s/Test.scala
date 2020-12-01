package com.sksamuel.avro4s

object Test extends App {
  Schema2[Foo]
}

case class Foo(a: String, b: Int)