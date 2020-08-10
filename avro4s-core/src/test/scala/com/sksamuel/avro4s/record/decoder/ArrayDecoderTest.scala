package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.AvroValue.AvroRecord
import com.sksamuel.avro4s._
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.language.higherKinds

case class TestVectorBooleans(booleans: Vector[Boolean])
case class TestArrayBooleans(booleans: Array[Boolean])
case class TestListBooleans(booleans: List[Boolean])
case class TestSetBooleans(booleans: Set[Boolean])
case class TestSetString(strings: Set[String])
case class TestSetDoubles(doubles: Set[Double])
case class TestSeqBooleans(booleans: Seq[Boolean])

case class TestArrayRecords(records: Array[Record])
case class TestSeqRecords(records: Seq[Record])
case class TestListRecords(records: List[Record])
case class TestSetRecords(records: Set[Record])
case class TestVectorRecords(records: Vector[Record])
case class Record(str: String, double: Double)

class ArrayDecoderTest extends AnyWordSpec with Matchers {

  import scala.collection.JavaConverters._

  "Decoder" should {

    "support array for a vector of primitives" in {
      val schema = AvroSchema[TestVectorBooleans]
      val record = new GenericData.Record(schema)
      record.put("booleans", List(true, false, true).asJava)
      Decoder[TestVectorBooleans].decode(AvroRecord(record)) shouldBe TestVectorBooleans(Vector(true, false, true))
    }

    "support array for an vector of records" in {

      val containerSchema = AvroSchema[TestVectorRecords]
      val recordSchema = AvroSchema[Record]

      val record1 = new GenericData.Record(recordSchema)
      record1.put("str", "qwe")
      record1.put("double", 123.4)

      val record2 = new GenericData.Record(recordSchema)
      record2.put("str", "wer")
      record2.put("double", 8234.324)

      val container = new GenericData.Record(containerSchema)
      container.put("records", List(record1, record2).asJava)

      Decoder[TestVectorRecords].decode(AvroRecord(container)) shouldBe TestVectorRecords(Vector(Record("qwe", 123.4), Record("wer", 8234.324)))
    }

    "support array for a scala.collection.immutable.Seq of primitives" in {
      case class Test(seq: Seq[String])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(Vector("a", "34", "fgD"))) shouldBe ImmutableRecord(schema, Vector(Vector(new Utf8("a"), new Utf8("34"), new Utf8("fgD")).asJava))
    }

    "support array for an Array of primitives" in {
      val schema = AvroSchema[TestArrayBooleans]
      val record = new GenericData.Record(schema)
      record.put("booleans", List(true, false, true).asJava)
      Decoder[TestArrayBooleans].decode(AvroRecord(record)).booleans.toVector shouldBe Vector(true, false, true)
    }

    "support array for a List of primitives" in {

      val schema = AvroSchema[TestListBooleans]
      val record = new GenericData.Record(schema)
      record.put("booleans", List(true, false, true).asJava)
      Decoder[TestListBooleans].decode(AvroRecord(record)) shouldBe TestListBooleans(List(true, false, true))
    }

    "support array for a List of records" in {

      val containerSchema = AvroSchema[TestListRecords]
      val recordSchema = AvroSchema[Record]

      val record1 = new GenericData.Record(recordSchema)
      record1.put("str", "qwe")
      record1.put("double", 123.4)

      val record2 = new GenericData.Record(recordSchema)
      record2.put("str", "wer")
      record2.put("double", 8234.324)

      val container = new GenericData.Record(containerSchema)
      container.put("records", List(record1, record2).asJava)

      Decoder[TestListRecords].decode(AvroRecord(container)) shouldBe TestListRecords(List(Record("qwe", 123.4), Record("wer", 8234.324)))
    }

    "support array for a scala.collection.immutable.Seq of records" in {

      val containerSchema = AvroSchema[TestSeqRecords]
      val recordSchema = AvroSchema[Record]

      val record1 = new GenericData.Record(recordSchema)
      record1.put("str", "qwe")
      record1.put("double", 123.4)

      val record2 = new GenericData.Record(recordSchema)
      record2.put("str", "wer")
      record2.put("double", 8234.324)

      val container = new GenericData.Record(containerSchema)
      container.put("records", List(record1, record2).asJava)

      Decoder[TestSeqRecords].decode(AvroRecord(container)) shouldBe TestSeqRecords(Seq(Record("qwe", 123.4), Record("wer", 8234.324)))
    }

    "support array for an Array of records" in {

      val containerSchema = AvroSchema[TestArrayRecords]
      val recordSchema = AvroSchema[Record]

      val record1 = new GenericData.Record(recordSchema)
      record1.put("str", "qwe")
      record1.put("double", 123.4)

      val record2 = new GenericData.Record(recordSchema)
      record2.put("str", "wer")
      record2.put("double", 8234.324)

      val container = new GenericData.Record(containerSchema)
      container.put("records", List(record1, record2).asJava)

      Decoder[TestArrayRecords].decode(AvroRecord(container)).records.toVector shouldBe Vector(Record("qwe", 123.4), Record("wer", 8234.324))
    }

    "support array for a Set of records" in {

      val containerSchema = AvroSchema[TestSetRecords]
      val recordSchema = AvroSchema[Record]

      val record1 = new GenericData.Record(recordSchema)
      record1.put("str", "qwe")
      record1.put("double", 123.4)

      val record2 = new GenericData.Record(recordSchema)
      record2.put("str", "wer")
      record2.put("double", 8234.324)

      val container = new GenericData.Record(containerSchema)
      container.put("records", List(record1, record2).asJava)

      Decoder[TestSetRecords].decode(AvroRecord(container)) shouldBe TestSetRecords(Set(Record("qwe", 123.4), Record("wer", 8234.324)))
    }
    "support array for a Set of strings" in {
      val schema = AvroSchema[TestSetString]
      val record = new GenericData.Record(schema)
      record.put("strings", List("Qwe", "324", "q").asJava)
      Decoder[TestSetString].decode(AvroRecord(record)) shouldBe TestSetString(Set("Qwe", "324", "q"))
    }
    "support array for a Set of doubles" in {
      val schema = AvroSchema[TestSetDoubles]
      val record = new GenericData.Record(schema)
      record.put("doubles", List(132.4324, 5.4, 0.123).asJava)
      Decoder[TestSetDoubles].decode(AvroRecord(record)) shouldBe TestSetDoubles(Set(132.4324, 5.4, 0.123))
    }
    //    "support Seq[Tuple2] issue #156" in {
    //      val schema = SchemaEncoder[TupleTest2]
    //    }
    //    "support Seq[Tuple3]" in {
    //      val schema = SchemaEncoder[TupleTest3]
    //    }

    "support top level Seq[Double]" in {
      Decoder[Seq[Double]].decode(AvroValue.unsafeFromAny(Array(1.2, 34.5, 54.3))) shouldBe Seq(1.2, 34.5, 54.3)
    }
    "support top level List[Int]" in {
      Decoder[List[Int]].decode(AvroValue.unsafeFromAny(Array(1, 4, 9))) shouldBe List(1, 4, 9)
    }
    "support top level Vector[String]" in {
      Decoder[Vector[String]].decode(AvroValue.unsafeFromAny(Array("a", "z"))) shouldBe Vector("a", "z")
    }
    "support top level Set[Boolean]" in {
      Decoder[Set[Boolean]].decode(AvroValue.unsafeFromAny(Array(true, false, true))) shouldBe Set(true, false)
    }
  }
}