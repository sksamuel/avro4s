package com.sksamuel.avro4s

import org.scalatest.{WordSpec, Matchers}

class SchemaBuilderTest extends WordSpec with Matchers {

  "SchemaGenerator.schemaFor" should {
    "accept booleans" in {
      case class Test(booly: Boolean)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/boolean.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept strings" in {
      case class Test(str: String)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/string.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept integer" in {
      case class Test(inty: Int)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/integer.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept longs" in {
      case class Test(foo: Long)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/long.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept double" in {
      case class Test(double: Double)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/double.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept float" in {
      case class Test(float: Float)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/float.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept nested case classes" in {
      case class Nested(goo: String)
      case class Test(foo: String, nested: Nested)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/nested.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept options as Union[T, Null]" in {
      case class Test(option: Option[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a scala.collection.immutable.Seq of primitives" in {
      case class Test(seq: Seq[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/seq.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for an Array of primitives" in {
      case class Test(array: Array[Boolean])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/array.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a List of primitives" in {
      case class Test(list: List[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/list.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a scala.collection.immutable.Seq of records" in {
      case class Nested(nested: String)
      case class Test(seq: Seq[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/seqrecords.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for an Array of records" in {
      case class Nested(nested: String)
      case class Test(array: Array[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/arrayrecords.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a List of records" in {
      case class Nested(nested: String)
      case class Test(list: List[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/listrecords.avsc"))
      val schema = SchemaBuilder[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}
