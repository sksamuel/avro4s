package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{AvroSchema, Encoder, ImmutableRecord}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{Matchers, WordSpec}

class ArrayEncoderTest extends WordSpec with Matchers {

  import scala.collection.JavaConverters._

  "Encoder" should {
    "generate array for a vector of primitives" in {
      case class Test(booleans: Vector[Boolean])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(Vector(true, false, true)), schema) shouldBe ImmutableRecord(schema, Vector(Vector(true, false, true).asJava))
    }
    "generate array for an vector of records" in {
      case class Test(records: Vector[Record])
      case class Record(str: String, double: Double)
      val schema = AvroSchema[Test]
      val rschema = AvroSchema[Record]
      Encoder[Test].encode(Test(Vector(Record("abc", 12.34))), schema) shouldBe ImmutableRecord(schema, Vector(Vector(ImmutableRecord(rschema, Vector(new Utf8("abc"), java.lang.Double.valueOf(12.34)))).asJava))
    }
    "generate array for a scala.collection.immutable.Seq of primitives" in {
      case class Test(seq: Seq[String])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(Vector("a", "fgD")), schema) shouldBe ImmutableRecord(schema, Vector(Vector(new Utf8("a"), new Utf8("fgD")).asJava))
    }
    "generate array for an Array of primitives" in {
      case class Test(array: Array[Boolean])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(Array(true, false, true)), schema) shouldBe ImmutableRecord(schema, Vector(Vector(true, false, true).asJava))
    }
    "generate array for a List of primitives" in {
      case class Test(list: List[String])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(List("we23", "54")), schema) shouldBe ImmutableRecord(schema, Vector(Vector(new Utf8("we23"), new Utf8("54")).asJava))
    }
    "generate array for a scala.collection.immutable.Seq of records" in {
      case class Nested(goo: String)
      case class Test(seq: Seq[Nested])
      val schema = AvroSchema[Test]
      val nschema = AvroSchema[Nested]
      Encoder[Test].encode(Test(Seq(Nested("qwe"), Nested("dfsg"))), schema) shouldBe ImmutableRecord(schema, Vector(Vector(ImmutableRecord(nschema, Vector(new Utf8("qwe"))), ImmutableRecord(nschema, Vector(new Utf8("dfsg")))).asJava))
    }
    "generate array for an Array of records" in {
      case class Nested(goo: String)
      case class Test(array: Array[Nested])
      val schema = AvroSchema[Test]
      val nschema = AvroSchema[Nested]
      Encoder[Test].encode(Test(Array(Nested("qwe"), Nested("dfsg"))), schema) shouldBe ImmutableRecord(schema, Vector(Vector(ImmutableRecord(nschema, Vector(new Utf8("qwe"))), ImmutableRecord(nschema, Vector(new Utf8("dfsg")))).asJava))
    }
    "generate array for a List of records" in {
      case class Nested(goo: String)
      case class Test(list: List[Nested])
      val schema = AvroSchema[Test]
      val nschema = AvroSchema[Nested]
      Encoder[Test].encode(Test(List(Nested("qwe"), Nested("dfsg"))), schema) shouldBe ImmutableRecord(schema, Vector(Vector(ImmutableRecord(nschema, Vector(new Utf8("qwe"))), ImmutableRecord(nschema, Vector(new Utf8("dfsg")))).asJava))

    }
    "generate array for a Set of records" in {
      case class Nested(goo: String)
      case class Test(set: Set[Nested])
      val schema = AvroSchema[Test]
      val nschema = AvroSchema[Nested]
      Encoder[Test].encode(Test(Set(Nested("qwe"), Nested("dfsg"))), schema) shouldBe ImmutableRecord(schema, Vector(Vector(ImmutableRecord(nschema, Vector(new Utf8("qwe"))), ImmutableRecord(nschema, Vector(new Utf8("dfsg")))).asJava))
    }
    "generate array for a Set of strings" in {
      case class Test(set: Set[String])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(Set("we23", "54")), schema) shouldBe ImmutableRecord(schema, Vector(Vector(new Utf8("we23"), new Utf8("54")).asJava))
    }
    "generate array for a Set of doubles" in {
      case class Test(set: Set[Double])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(Set(1.2, 34.5, 54.3)), schema) shouldBe ImmutableRecord(schema, Vector(Vector(1.2, 34.5, 54.3).asJava))
    }
    //    "support Seq[Tuple2] issue #156" in {
    //      val schema = SchemaEncoder[TupleTest2]
    //    }
    //    "support Seq[Tuple3]" in {
    //      val schema = SchemaEncoder[TupleTest3]
    //    }
    "support top level Seq[Double]" in {
      val schema = AvroSchema[Array[Double]]
      Encoder[Array[Double]].encode(Array(1.2, 34.5, 54.3), schema) shouldBe new GenericData.Array[Double](schema, List(1.2, 34.5, 54.3).asJava)
    }
    "support top level List[Int]" in {
      val schema = AvroSchema[List[Int]]
      Encoder[List[Int]].encode(List(1, 4, 9), schema) shouldBe new GenericData.Array[Int](schema, List(1, 4, 9).asJava)
    }
    "support top level Vector[String]" in {
      val schema = AvroSchema[Vector[String]]
      Encoder[Vector[String]].encode(Vector("a", "z"), schema) shouldBe new GenericData.Array[Utf8](schema, List(new Utf8("a"), new Utf8("z")).asJava)
    }
    "support top level Set[Boolean]" in {
      val schema = AvroSchema[Set[Boolean]]
      Encoder[Set[Boolean]].encode(Set(true, false, true), schema) shouldBe new GenericData.Array[Boolean](schema, List(true, false).asJava)
    }
  }
}

case class TupleTest2(first: String, second: Seq[(TupleTestA, TupleTestB)])
case class TupleTest3(first: String, second: Seq[(TupleTestA, TupleTestB, TupleTestC)])
case class TupleTestA(parameter: Int)
case class TupleTestB(parameter: Int)
case class TupleTestC(parameter: Int)