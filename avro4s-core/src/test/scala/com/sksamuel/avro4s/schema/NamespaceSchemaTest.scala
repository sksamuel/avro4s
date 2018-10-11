package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.{FunSuite, Matchers}

class NamespaceSchemaTest extends FunSuite with Matchers {

  test("use package name for top level class") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_class_namespace.json"))
    val schema = AvroSchema[Tau]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("use package name without .package for classes defined in the package object") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_package_object_namespace.json"))
    val schema = AvroSchema[Sigma]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("use namespace of object for classes defined inside an object") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_object_namespace.json"))
    val schema = AvroSchema[A]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("local classes should use the namespace of their parent object package") {
    case class NamespaceTestFoo(inner: String)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/local_class_namespace.json"))
    val schema = AvroSchema[NamespaceTestFoo]
    schema.toString(true) shouldBe expected.toString(true)
  }
}


case class Tau(a: String, b: Boolean)

case class A(inner: A.Inner)
object A {
  final case class Inner(s: String)
}
