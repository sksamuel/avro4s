package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.AvroSchema
import org.scalatest.{FunSuite, Matchers}

case class Outer(inner: Outer.Inner)
object Outer {
  final case class Inner(s: String)
}

class ObjectNamespaceTest extends FunSuite with Matchers {

  case class A(inner: A.B)
  object A {
    final case class B(s: String)
  }

  test("object namespaces at the top level") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/object_namespaces.json"))
    val schema = AvroSchema[Outer]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("local object namespaces") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/object_namespaces_local.json"))
    val schema = AvroSchema[A]
    schema.toString(true) shouldBe expected.toString(true)
  }
}
