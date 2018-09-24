package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.internal.AvroSchema
import org.scalatest.{Matchers, WordSpec}

class ArraySchemaTest extends WordSpec with Matchers {

  "SchemaEncoder" should {
    "generate array type for a vector of primitives" in {
      case class VectorPrim(booleans: Vector[Boolean])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/vector_prim.avsc"))
      val schema = AvroSchema[VectorPrim]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for an vector of records" in {
      case class VectorRecord(records: Vector[Record])
      case class Record(str: String, double: Double)
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/vector_records.avsc"))
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
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/list.avsc"))
      val schema = AvroSchema[NestedListString]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a scala.collection.immutable.Seq of records" in {
      case class Nested(goo: String)
      case class Test(seq: Seq[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/seqrecords.avsc"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for an Array of records" in {
      case class Nested(goo: String)
      case class Test(array: Array[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/arrayrecords.avsc"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a List of records" in {
      case class Nested(goo: String)
      case class Test(list: List[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/listrecords.avsc"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a Set of records" in {
      case class Nested(goo: String)
      case class NestedSet(set: Set[Nested])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/setrecords.avsc"))
      val schema = AvroSchema[NestedSet]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a Set of strings" in {
      case class Test(set: Set[String])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/setstrings.avsc"))
      val schema = AvroSchema[Test]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a Set of doubles" in {
      case class NestedSetDouble(set: Set[Double])
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/setdoubles.avsc"))
      val schema = AvroSchema[NestedSetDouble]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support Seq[Tuple2] issue #156" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/tuple2.json"))
      val schema = AvroSchema[TupleTest2]
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support Seq[Tuple3]" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/tuple3.json"))
      val schema = AvroSchema[TupleTest3]
      schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class TupleTest2(first: String, second: Seq[(TupleTestA, TupleTestB)])
case class TupleTest3(first: String, second: Seq[(TupleTestA, TupleTestB, TupleTestC)])
case class TupleTestA(parameter: Int)
case class TupleTestB(parameter: Int)
case class TupleTestC(parameter: Int)