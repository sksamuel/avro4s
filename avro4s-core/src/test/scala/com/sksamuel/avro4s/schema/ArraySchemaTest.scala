package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.{Matchers, WordSpec}

class ArraySchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "generate array type for a vector of primitives" in {
      case class VectorPrim(booleans: Vector[Boolean])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/vector_prim.json"))
      val schema = AvroSchema[VectorPrim]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for an vector of records" in {
      case class VectorRecord(records: Vector[Record])
      case class Record(str: String, double: Double)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/vector_records.json"))
      val schema = AvroSchema[VectorRecord]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a scala.collection.immutable.Seq of primitives" in {
      case class Test(seq: Seq[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/seq.avsc"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for an Array of primitives" in {
      case class Test(array: Array[Boolean])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/array.avsc"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a List of primitives" in {
      case class NestedListString(list: List[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/list.json"))
      val schema = AvroSchema[NestedListString]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a scala.collection.immutable.Seq of records" in {
      case class Nested(goo: String)
      case class Test(seq: Seq[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/seqrecords.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for an Array of records" in {
      case class Nested(goo: String)
      case class Test(array: Array[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/arrayrecords.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a List of records" in {
      case class Nested(goo: String)
      case class Test(list: List[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/listrecords.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a Set of records" in {
      case class Nested(goo: String)
      case class NestedSet(set: Set[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/setrecords.json"))
      val schema = AvroSchema[NestedSet]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a Set of strings" in {
      case class Test(set: Set[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/setstrings.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a Set of doubles" in {
      case class NestedSetDouble(set: Set[Double])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/setdoubles.json"))
      val schema = AvroSchema[NestedSetDouble]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support Seq[Tuple2] issue #156" in {
      case class TupleTest2(first: String, second: Seq[(TupleTestA, TupleTestB)])
      case class TupleTestA(parameter: Int)
      case class TupleTestB(parameter: Int)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/tuple2.json"))
      val schema = AvroSchema[TupleTest2]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support Seq[Tuple3]" in {
      case class TupleTest3(first: String, second: Seq[(TupleTestA, TupleTestB, TupleTestC)])
      case class TupleTestA(parameter: Int)
      case class TupleTestB(parameter: Int)
      case class TupleTestC(parameter: Int)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/tuple3.json"))
      val schema = AvroSchema[TupleTest3]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Seq[Double]" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_seq_double.json"))
      val schema = AvroSchema[Array[Double]]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level List[Int]" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_list_int.json"))
      val schema = AvroSchema[List[Int]]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Vector[String]" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_vector_string.json"))
      val schema = AvroSchema[Vector[String]]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support top level Set[Boolean]" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/top_level_set_boolean.json"))
      val schema = AvroSchema[Set[Boolean]]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support array of maps" in {
      case class Test(array: Array[Map[String, String]])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/array_of_maps.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support lists of maps" in {
      case class Test(list: List[Map[String, String]])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/list_of_maps.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support seq of maps" in {
      case class Test(seq: Seq[Map[String, String]])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/seq_of_maps.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support vector of maps" in {
      case class Test(vector: Vector[Map[String, String]])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/vector_of_maps.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support case class of list of maps" in {
      case class Ship(map: scala.collection.immutable.Map[String, String])
      case class Test(ship: List[scala.collection.immutable.Map[String, String]])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/class_with_list_of_maps.json"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

