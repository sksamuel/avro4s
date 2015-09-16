package com.sksamuel.avro4s

object SchemaTest extends App {
  println(Macros.materializeWriter[Person].schema)
  println(Macros.materializeWriter[Song].schema)
}

case class Person(name: String, age: Int)
case class Song(title: String, artists: Seq[String])
