package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class NestedClassesTest extends AnyWordSpec with Matchers {

  "SchemaFor" should {
    "accept nested case classes" in {
      case class Nested(goo: String)
      case class NestedTest(foo: String, nested: Nested)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/nested/nested.json"))
      val schema = AvroSchema[NestedTest]
      schema.toString(true) shouldBe expected.toString(true)
    }

    "accept multiple nested case classes" in {
      case class Inner(goo: String)
      case class Middle(inner: Inner)
      case class Outer(middle: Middle)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/nested/multiple.json"))
      val schema = AvroSchema[Outer]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept deep nested structure" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/schemas/nested/deepnested.json"))
      val schema = AvroSchema[Level1]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class Level4(str: String)
case class Level3(level4: Level4)
case class Level2(level3: Level3)
case class Level1(level2: Level2)