package com.sksamuel.avro4s

@AvroSpecificGeneric(enabled = true)
case class MyWrapper[A](a: A)

@AvroSpecificGeneric(enabled = false)
case class DisabledWrapper[A](a: A)

case class NotAnnotatedWrapper[A](a: A)

case class MyData(i: MyWrapper[Int], s: Option[MyWrapper[String]])

case class DisabledMyData(i: DisabledWrapper[Int], s: Option[DisabledWrapper[String]])

case class NotAnnotatedMyData(i: NotAnnotatedWrapper[Int], s: Option[NotAnnotatedWrapper[String]])
