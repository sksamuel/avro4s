package com.sksamuel.avro4s

case class MyWrapper[A](a: A)
case class MyData(i: MyWrapper[Int], s: Option[MyWrapper[String]])

