package com.sksamuel.avro4s

object SchemaTest extends App {
  val schema = Macros.materializeSchema[Person]
  println(schema.s)
}

case class Person(name: String, age: Int)
