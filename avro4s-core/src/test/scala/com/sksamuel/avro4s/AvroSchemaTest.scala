package com.sksamuel.avro4s

import org.scalatest.{Matchers, WordSpec}

sealed trait Wibble
case class Wobble(str: String) extends Wibble
case class Wabble(dbl: Double) extends Wibble
case class Wrapper(wibble: Wibble)

sealed trait Tibble
case class Tobble(str: String, place: String) extends Tibble
case class Tabble(str: Double, age: Int) extends Tibble
case class Trapper(tibble: Tibble)

sealed trait Nibble
case class Nobble(str: String, place: String) extends Nibble
case class Nabble(str: String, age: Int) extends Nibble
case class Napper(nibble: Nibble)

class AvroSchemaTest extends WordSpec with Matchers {

  case class Nested(goo: String)

  case class NestedBoolean(b: Boolean)

  case class NestedTest(foo: String, nested: Nested)

  case class Inner(goo: String)

  case class Middle(inner: Inner)

  case class Outer(middle: Middle)

  "AvroSchema" should {
    "accept booleans" in {
      case class Test(booly: Boolean)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/boolean.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept bytes" in {
      case class Test(bytes: Array[Byte])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bytes.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept strings" in {
      case class Test(str: String)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/string.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept integer" in {
      case class Test(inty: Int)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/integer.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept longs" in {
      case class Test(foo: Long)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/long.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept double" in {
      case class Test(double: Double)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/double.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept float" in {
      case class Test(float: Float)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/float.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept big decimal" in {
      case class Test(decimal: BigDecimal)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept nested case classes" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/nested.avsc"))
      val schema = SchemaFor[NestedTest]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept multiple nested case classes" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/nested_multiple.avsc"))
      val schema = SchemaFor[Outer]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate option as Union[T, Null]" in {
      case class Test(option: Option[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/option.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a scala.collection.immutable.Seq of primitives" in {
      case class Test(seq: Seq[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/seq.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for an Array of primitives" in {
      case class Test(array: Array[Boolean])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/array.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a List of primitives" in {
      case class Test(list: List[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/list.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a scala.collection.immutable.Seq of records" in {
      case class Test(seq: Seq[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/seqrecords.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for an Array of records" in {
      case class Test(array: Array[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/arrayrecords.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a List of records" in {
      case class Test(list: List[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/listrecords.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a Set of records" in {
      case class Test(set: Set[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/setrecords.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a Set of strings" in {
      case class Test(set: Set[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/setstrings.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate union:T,U for Either[T,U] of primitives" in {
      case class Test(either: Either[String, Double])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/either.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate union:T,U for Either[T,U] of records" in {
      case class Test(either: Either[Nested, NestedBoolean])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/eitherrecord.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate map type for a scala.collection.immutable.Map of primitives" in {
      case class Test(map: Map[String, String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/map.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate map type for a scala.collection.immutable.Map of records" in {
      case class Test(map: Map[String, Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/maprecord.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate map type for a scala.collection.immutable.Map of Option[Boolean]" in {
      case class Test(map: Map[String, Option[Boolean]])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/mapoption.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support maps of seqs of records" in {
      case class Test(map: Map[String, Seq[Nested]])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/map_seq_nested.avsc"))
      val schema = SchemaFor[Test]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept java enums" in {
      case class Test(wine: Wine)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/enum.avsc"))
      val schema = SchemaFor[Test]()
      println(schema.toString(true))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support sealed traits" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/sealed_traits.avsc"))
      val schema = SchemaFor[Wrapper]()
      println(schema.toString(true))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "merge trait subtypes fields with same name into unions" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/trait_subtypes_duplicate_fields.avsc"))
      val schema = SchemaFor[Trapper]()
      println(schema.toString(true))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "merge trait subtypes fields with same name and same type into a single schema" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/trait_subtypes_duplicate_fields_same_type.avsc"))
      val schema = SchemaFor[Napper]()
      println(schema.toString(true))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support doc annotation on class" in {
      @AvroDoc("hello its me") case class Annotated(str: String)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/doc_annotation_class.avsc"))
      val schema = SchemaFor[Annotated]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support doc annotation on field" in {
      case class Annotated(@AvroDoc("hello its me") str: String, @AvroDoc("I am a long") long: Long, int: Int)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/doc_annotation_field.avsc"))
      val schema = SchemaFor[Annotated]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support prop annotation on class" in {
      @AvroProp("cold", "play") case class Annotated(str: String)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/props_annotation_class.avsc"))
      val schema = SchemaFor[Annotated]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support prop annotation on field" in {
      case class Annotated(@AvroProp("cold", "play") str: String, @AvroProp("kate", "bush") long: Long, int: Int)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/props_annotation_field.avsc"))
      val schema = SchemaFor[Annotated]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support alias annotations on field" in {
      case class Annotated(@AvroAlias("cold") str: String, @AvroAlias("kate") @AvroAlias("bush") long: Long, int: Int)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/aliases.avsc"))
      val schema = SchemaFor[Annotated]()
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

