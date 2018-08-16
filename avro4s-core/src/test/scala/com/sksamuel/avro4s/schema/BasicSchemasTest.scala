package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s._
import com.sksamuel.avro4s.internal.SchemaEncoder
import org.scalatest.{Matchers, WordSpec}

class BasicSchemasTest extends WordSpec with Matchers {
  "SchemaEncoder" should {
    "accept booleans" in {
      case class Test(booly: Boolean)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/boolean.avsc"))
      val schema = SchemaEncoder[Test].encode
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept bytes" in {
      case class Test(bytes: Array[Byte])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bytes.avsc"))
      val schema = SchemaEncoder[Test].encode
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept strings" in {
      case class Test(str: String)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/string.avsc"))
      val schema = SchemaEncoder[Test].encode
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept integer" in {
      case class Test(inty: Int)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/integer.avsc"))
      val schema = SchemaEncoder[Test].encode
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept longs" in {
      case class Test(foo: Long)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/long.avsc"))
      val schema = SchemaEncoder[Test].encode
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept double" in {
      case class Test(double: Double)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/double.avsc"))
      val schema = SchemaEncoder[Test].encode
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept float" in {
      case class Test(float: Float)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/float.avsc"))
      val schema = SchemaEncoder[Test].encode
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support recursive types" in {
      val schema = SchemaFor[Recursive]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/recursive.avsc"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support mutually recursive types" in {
      val schema = SchemaFor[MutRec1]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/mutrec.avsc"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support types nested in uppercase packages" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/nested_in_uppercase_pkg.avsc"))
      val schema = SchemaFor[examples.UppercasePkg.Data]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support default values" in {
      val schema = SchemaFor[DefaultValues]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/defaultvalues.avsc"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept nested case classes" in {
      case class Nested(goo: String)
      case class NestedTest(foo: String, nested: Nested)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/nested.avsc"))
      val schema = SchemaFor[NestedTest]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept multiple nested case classes" in {
      case class Inner(goo: String)
      case class Middle(inner: Inner)
      case class Outer(middle: Middle)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/nested_multiple.avsc"))
      val schema = SchemaFor[Outer]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept deep nested structure" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/deepnested.avsc"))
      val schema = SchemaFor[Level1]()
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}


case class Level4(str: Map[String, String])
case class Level3(level4: Level4)
case class Level2(level3: Level3)
case class Level1(level2: Level2)