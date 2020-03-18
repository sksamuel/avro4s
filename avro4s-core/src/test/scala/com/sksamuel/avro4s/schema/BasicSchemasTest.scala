package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchemaV2
import com.sksamuel.avro4s.examples.UppercasePkg.ClassInUppercasePackage
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BasicSchemasTest extends AnyWordSpec with Matchers {

  "SchemaEncoder" should {
    "accept booleans" in {
      case class Test(booly: Boolean)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/boolean.avsc"))
      val schema = AvroSchemaV2[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept bytes" in {
      case class Test(bytes: Array[Byte])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bytes.avsc"))
      val schema = AvroSchemaV2[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept strings" in {
      case class Test(str: String)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/string.json"))
      val schema = AvroSchemaV2[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept integer" in {
      case class Test(inty: Int)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/integer.avsc"))
      val schema = AvroSchemaV2[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept longs" in {
      case class Test(foo: Long)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/long.json"))
      val schema = AvroSchemaV2[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept double" in {
      case class Test(double: Double)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/double.json"))
      val schema = AvroSchemaV2[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept float" in {
      case class Test(float: Float)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/float.avsc"))
      val schema = AvroSchemaV2[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    // todo fix
    "support simple recursive types" ignore {
      val schema = AvroSchemaV2[RecursiveFoo]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/recursive.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    // todo fix
    "support recursive ADTs" ignore {
      val schema = AvroSchemaV2[Tree[String]]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/recursive_adt.json"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    // todo fix
    //    "support mutually recursive types" ignore {
    //      val schema = AvroSchemaV2[MutRec1]
    //      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/mutrec.json"))
    //      schema.toString(true) shouldBe expected.toString(true)
    //    }
    "support types nested in uppercase packages" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/nested_in_uppercase_pkg.json"))
      val schema = AvroSchemaV2[ClassInUppercasePackage]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept nested case classes" in {
      case class Nested(goo: String)
      case class NestedTest(foo: String, nested: Nested)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/nested.json"))
      val schema = AvroSchemaV2[NestedTest]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept multiple nested case classes" in {
      case class Inner(goo: String)
      case class Middle(inner: Inner)
      case class Outer(middle: Middle)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/nested_multiple.json"))
      val schema = AvroSchemaV2[Outer]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept deep nested structure" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/deepnested.json"))
      val schema = AvroSchemaV2[Level1]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class Level4(str: Map[String, String])
case class Level3(level4: Level4)
case class Level2(level3: Level3)
case class Level1(level2: Level2)

case class RecursiveFoo(list: Seq[RecursiveFoo])

case class MutRec1(payload: Int, children: List[MutRec2])
case class MutRec2(payload: String, children: List[MutRec1])

sealed trait Tree[+T]
case class Branch[+T](left: Tree[T], right: Tree[T]) extends Tree[T]
case class Leaf[+T](value: T) extends Tree[T]