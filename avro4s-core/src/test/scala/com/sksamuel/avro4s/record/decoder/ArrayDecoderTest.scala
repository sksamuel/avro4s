package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.internal.{Decoder, Encoder, InternalRecord, AvroSchema}
import org.apache.avro.generic.GenericData
import org.scalatest.{Matchers, WordSpec}

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

class ArrayDecoderTest extends WordSpec with Matchers {

  import scala.collection.JavaConverters._

  "Decoder" should {

    "support array for a vector of primitives" in {
      val schema = AvroSchema[TestVectorBooleans]
      val record = new GenericData.Record(schema)
      record.put("booleans", List(true, false, true).asJava)
      Decoder[TestVectorBooleans].decode(record) shouldBe TestVectorBooleans(Vector(true, false, true))
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

      Decoder[TestVectorRecords].decode(container) shouldBe TestVectorRecords(Vector(Record("qwe", 123.4), Record("wer", 8234.324)))
    }

    "support array for a scala.collection.immutable.Seq of primitives" in {
      case class Test(seq: Seq[String])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(Vector("a", "34", "fgD")), schema) shouldBe InternalRecord(schema, Vector(Vector("a", "34", "fgD").asJava))
    }

    "support array for an Array of primitives" in {
      val schema = AvroSchema[TestArrayBooleans]
      val record = new GenericData.Record(schema)
      record.put("booleans", List(true, false, true).asJava)
      Decoder[TestArrayBooleans].decode(record).booleans.toVector shouldBe Vector(true, false, true)
    }

    "support array for a List of primitives" in {

      val schema = AvroSchema[TestListBooleans]
      val record = new GenericData.Record(schema)
      record.put("booleans", List(true, false, true).asJava)
      Decoder[TestListBooleans].decode(record) shouldBe TestListBooleans(List(true, false, true))
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

      Decoder[TestListRecords].decode(container) shouldBe TestListRecords(List(Record("qwe", 123.4), Record("wer", 8234.324)))
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

      Decoder[TestSeqRecords].decode(container) shouldBe TestSeqRecords(Seq(Record("qwe", 123.4), Record("wer", 8234.324)))
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

      Decoder[TestArrayRecords].decode(container).records.toVector shouldBe Vector(Record("qwe", 123.4), Record("wer", 8234.324))
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

      Decoder[TestSetRecords].decode(container) shouldBe TestSetRecords(Set(Record("qwe", 123.4), Record("wer", 8234.324)))
    }

    "support array for a Set of strings" in {
      val schema = AvroSchema[TestSetString]
      val record = new GenericData.Record(schema)
      record.put("strings", List("Qwe", "324", "q").asJava)
      Decoder[TestSetString].decode(record) shouldBe TestSetString(Set("Qwe", "324", "q"))
    }

    "support array for a Set of doubles" in {
      val schema = AvroSchema[TestSetDoubles]
      val record = new GenericData.Record(schema)
      record.put("doubles", List(132.4324, 5.4, 0.123).asJava)
      Decoder[TestSetDoubles].decode(record) shouldBe TestSetDoubles(Set(132.4324, 5.4, 0.123))
    }
    //    "support Seq[Tuple2] issue #156" in {
    //      val schema = SchemaEncoder[TupleTest2]
    //    }
    //    "support Seq[Tuple3]" in {
    //      val schema = SchemaEncoder[TupleTest3]
    //    }
  }
}