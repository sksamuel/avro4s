package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.AvroSchema
import org.scalatest.{Matchers, WordSpec}

class NamespaceSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "use package name for top level class" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_class_namespace.json"))
      val schema = AvroSchema[Tau]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "use package name without .package for classes defined in the package object" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_package_object_namespace.json"))
      val schema = AvroSchema[Sigma]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "use namespace of object for classes defined inside an object" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_object_namespace.json"))
      val schema = AvroSchema[A]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "use namespace of local object for classes defined inside" in {
      case class Foo(inner: Boo.Inner)
      object Boo {
        final case class Inner(s: String)
      }
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/local_object_namespace.json"))
      val schema = AvroSchema[Foo]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class Tau(a: String, b: Boolean)

case class A(inner: A.Inner)
object A {
  final case class Inner(s: String)
}
