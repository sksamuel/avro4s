package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.SchemaFor
import org.scalatest.{Matchers, WordSpec}

class NamespaceSchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "use namespace of object for classes defined inside an object" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_object_namespace.json"))
      val schema = SchemaFor[A]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "use namespace of local object for classes defined inside" in {
      case class Foo(inner: Boo.Inner)
      object Boo {
        final case class Inner(s: String)
      }
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/local_object_namespace.json"))
      val schema = SchemaFor[Foo]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class A(inner: A.Inner)
object A {
  final case class Inner(s: String)
}
