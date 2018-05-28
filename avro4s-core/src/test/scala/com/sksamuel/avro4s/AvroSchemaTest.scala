package com.sksamuel.avro4s

import java.time.LocalDate
import java.util.UUID

import org.scalatest.{Matchers, WordSpec}
import shapeless.{:+:, CNil}

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

case class Level4(str: Map[String, String])
case class Level3(level4: Level4)
case class Level2(level3: Level3)
case class Level1(level2: Level2)

case class Ids(myid: UUID)

case class LocalDateTest(localDate: LocalDate)

case class Recursive(payload: Int, next: Option[Recursive])

case class MutRec1(payload: Int, children: List[MutRec2])
case class MutRec2(payload: String, children: List[MutRec1])

case class Union(union: Int :+: String :+: Boolean :+: CNil)
case class UnionOfUnions(union: (Int :+: String :+: CNil) :+: Boolean :+: CNil)
case class OptionalUnion(union: Option[Int :+: String :+: CNil])
case class UnionOfOptional(union: Option[Int] :+: String :+: CNil)
case class AllOptionals(union: Option[Option[Int] :+: Option[String] :+: CNil])

class AvroSchemaTest extends WordSpec with Matchers {

  case class NestedListString(list: List[String])
  case class NestedSetDouble(set: Set[Double])
  case class NestedSet(set: Set[Nested])
  case class Nested(goo: String)
  case class NestedBoolean(b: Boolean)
  case class NestedTest(foo: String, nested: Nested)
  case class Inner(goo: String)
  case class Middle(inner: Inner)
  case class Outer(middle: Middle)

  "AvroSchema" should {
    "support top level Doubles" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_double.avsc"))
      val schema = AvroSchema[Double]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Booleans" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_boolean.avsc"))
      val schema = AvroSchema[Boolean]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Longs" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_long.avsc"))
      val schema = AvroSchema[Long]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Integers" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_integer.avsc"))
      val schema = AvroSchema[Int]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Strings" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_string.avsc"))
      val schema = AvroSchema[String]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Floats" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_float.avsc"))
      val schema = AvroSchema[Float]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept booleans" in {
      case class Test(booly: Boolean)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/boolean.avsc"))
      val schema = AvroSchema[Test]
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
      val schema = SchemaFor[Test]()
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/bigdecimal.avsc"))
      schema shouldBe expected
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
    "accept LocalDate" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/localdate.avsc"))
      val schema = SchemaFor[LocalDateTest]()
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
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/list.avsc"))
      val schema = SchemaFor[NestedListString]()
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
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/setrecords.avsc"))
      val schema = SchemaFor[NestedSet]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a Set of strings" in {
      case class Test(set: Set[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/setstrings.avsc"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a Set of doubles" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/setdoubles.avsc"))
      val schema = SchemaFor[NestedSetDouble]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "accept deep nested structure" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/deepnested.avsc"))
      val schema = SchemaFor[Level1]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate union:T,U for Either[T,U] of primitives" in {
      case class Test(either: Either[String, Double])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/either.avsc"))
      val schema = AvroSchema[Test]
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

    "support sealed traits" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/sealed_traits.avsc"))
      val schema = SchemaFor[Wrapper]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support trait subtypes fields with same name" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/trait_subtypes_duplicate_fields.avsc"))
      val schema = SchemaFor[Trapper]()
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support trait subtypes fields with same name and same type" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/trait_subtypes_duplicate_fields_same_type.avsc"))
      val schema = SchemaFor[Napper]()
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
  "support scala enums" in {
    val schema = SchemaFor[ScalaEnums]()
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/scalaenums.avsc"))
    schema.toString(true) shouldBe expected.toString(true)
  }
  "support default values" in {
    val schema = SchemaFor[DefaultValues]()
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/defaultvalues.avsc"))
    schema.toString(true) shouldBe expected.toString(true)
  }
  "support default option values" in {
    val schema = SchemaFor[OptionDefaultValues]()
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optiondefaultvalues.avsc"))
    schema.toString(true) shouldBe expected.toString(true)
  }
  "support default options of scala enum values" in {
    val schema = SchemaFor[ScalaOptionEnums]()
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optionscalaenum.avsc"))
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
  "generate schema for underlying field in a value class" in {
    val schema = SchemaFor[ValueClass]()
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/value_class.avsc"))
    schema.toString(true) shouldBe expected.toString(true)
  }
  "support unions and unions of unions" in {
    val single = SchemaFor[Union]()
    val unionOfUnions = SchemaFor[UnionOfUnions]()

    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/union.avsc"))

    single.toString(true) shouldBe expected.toString(true)
    unionOfUnions.toString(true) shouldBe expected.toString(true).replace("Union", "UnionOfUnions")
  }
  "support mixing optionals with unions, merging appropriately" in {
    val outsideOptional = SchemaFor[OptionalUnion]()
    val insideOptional = SchemaFor[UnionOfOptional]()
    val bothOptional = SchemaFor[AllOptionals]()

    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/optionalunion.avsc"))

    outsideOptional.toString(true) shouldBe expected.toString(true)
    insideOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "UnionOfOptional")
    bothOptional.toString(true) shouldBe expected.toString(true).replace("OptionalUnion", "AllOptionals")
  }
  "generate array type for a vector of primitives" in {
    case class VectorPrim(booleans: Vector[Boolean])
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/vector_prim.avsc"))
    val schema = SchemaFor[VectorPrim]()
    schema.toString(true) shouldBe expected.toString(true)
  }
  "generate array type for an vector of records" in {
    case class VectorRecord(records: Vector[Record])
    case class Record(str: String, double: Double)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/vector_records.avsc"))
    val schema = SchemaFor[VectorRecord]()
    schema.toString(true) shouldBe expected.toString(true)
  }
  "support types nested in uppercase packages" in {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/nested_in_uppercase_pkg.avsc"))
    val schema = SchemaFor[examples.UppercasePkg.Data]()
    schema.toString(true) shouldBe expected.toString(true)
  }
  "support Seq[Tuple2] issue #156" in {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/tuple2.json"))
    val schema = SchemaFor[TupleTest2]()
    schema.toString(true) shouldBe expected.toString(true)
  }
  "support Seq[Tuple3]" in {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/tuple3.json"))
    val schema = SchemaFor[TupleTest3]()
    schema.toString(true) shouldBe expected.toString(true)
  }

  case class TupleTest2(first: String, second: Seq[(TupleTestA, TupleTestB)])
  case class TupleTest3(first: String, second: Seq[(TupleTestA, TupleTestB, TupleTestC)])
  case class TupleTestA(parameter: Int)
  case class TupleTestB(parameter: Int)
  case class TupleTestC(parameter: Int)
}

case class OptionDefaultValues(
  name: String = "sammy",
  description: Option[String] = None,
  currency: Option[String] = Some("$")
)

case class DefaultValues(
  name: String = "sammy",
  age: Int = 21,
  isFemale: Boolean = false,
  length: Double = 6.2,
  timestamp: Long = 1468920998000l,
  address: Map[String, String] = Map(
    "home" -> "sammy's home address",
    "work" -> "sammy's work address"
  ),
  traits: Seq[String] = Seq("Adventurous", "Helpful"),
  favoriteWine: Wine = Wine.CabSav
)

