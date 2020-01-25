package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s._
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ArrayEncoderTest extends AnyWordSpec with Matchers {

  import scala.collection.JavaConverters._

  "Encoder" should {
    "generate array for a vector of primitives" in {
      case class Test(booleans: Vector[Boolean])
      val schema = AvroSchemaV2[Test]
      EncoderV2[Test].encode(Test(Vector(true, false, true))) shouldBe ImmutableRecord(schema, Vector(Vector(true, false, true).asJava))
    }
    "generate array for an vector of records" in {
      case class Test(records: Vector[Record])
      case class Record(str: String, double: Double)
      val schema = AvroSchemaV2[Test]
      val rschema = AvroSchemaV2[Record]
      EncoderV2[Test].encode(Test(Vector(Record("abc", 12.34)))) shouldBe ImmutableRecord(schema, Vector(Vector(ImmutableRecord(rschema, Vector(new Utf8("abc"), java.lang.Double.valueOf(12.34)))).asJava))
    }
    "generate array for a scala.collection.immutable.Seq of primitives" in {
      case class Test(seq: Seq[String])
      val schema = AvroSchemaV2[Test]
      EncoderV2[Test].encode(Test(Vector("a", "fgD"))) shouldBe ImmutableRecord(schema, Vector(Vector(new Utf8("a"), new Utf8("fgD")).asJava))
    }
    "generate array for an Array of primitives" in {
      case class Test(array: Array[Boolean])
      val schema = AvroSchemaV2[Test]
      EncoderV2[Test].encode(Test(Array(true, false, true))) shouldBe ImmutableRecord(schema, Vector(Vector(true, false, true).asJava))
    }
    "generate array for a List of primitives" in {
      case class Test(list: List[String])
      val schema = AvroSchema[Test]
      Encoder[Test].encode(Test(List("we23", "54")), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(Vector(new Utf8("we23"), new Utf8("54")).asJava))
    }
    "generate array for a scala.collection.immutable.Seq of records" in {
      case class Nested(goo: String)
      case class Test(seq: Seq[Nested])
      val schema = AvroSchemaV2[Test]
      val nschema = AvroSchemaV2[Nested]
      EncoderV2[Test].encode(Test(Seq(Nested("qwe"), Nested("dfsg")))) shouldBe ImmutableRecord(schema, Vector(Vector(ImmutableRecord(nschema, Vector(new Utf8("qwe"))), ImmutableRecord(nschema, Vector(new Utf8("dfsg")))).asJava))
    }
    "generate array for an Array of records" in {
      case class Nested(goo: String)
      case class Test(array: Array[Nested])
      val schema = AvroSchemaV2[Test]
      val nschema = AvroSchemaV2[Nested]
      EncoderV2[Test].encode(Test(Array(Nested("qwe"), Nested("dfsg")))) shouldBe ImmutableRecord(schema, Vector(Vector(ImmutableRecord(nschema, Vector(new Utf8("qwe"))), ImmutableRecord(nschema, Vector(new Utf8("dfsg")))).asJava))
    }
    "generate array for a List of records" in {
      case class Nested(goo: String)
      case class Test(list: List[Nested])
      val schema = AvroSchemaV2[Test]
      val nschema = AvroSchemaV2[Nested]
      EncoderV2[Test].encode(Test(List(Nested("qwe"), Nested("dfsg")))) shouldBe ImmutableRecord(schema, Vector(Vector(ImmutableRecord(nschema, Vector(new Utf8("qwe"))), ImmutableRecord(nschema, Vector(new Utf8("dfsg")))).asJava))
    }
    "generate array for a Set of records" in {
      case class Nested(goo: String)
      case class Test(set: Set[Nested])
      val nschema = AvroSchemaV2[Nested]
      val record = EncoderV2[Test].encode(Test(Set(Nested("qwe"), Nested("dfsg")))).asInstanceOf[ImmutableRecord]
      record.values(0).asInstanceOf[java.util.Collection[Nested]].asScala.toSet shouldBe Set(ImmutableRecord(nschema, Vector(new Utf8("qwe"))), ImmutableRecord(nschema, Vector(new Utf8("dfsg"))))
    }
    "generate array for a Set of strings" in {
      case class Test(set: Set[String])
      val schema = AvroSchemaV2[Test]
      EncoderV2[Test].encode(Test(Set("we23", "54"))) shouldBe ImmutableRecord(schema, Vector(Vector(new Utf8("we23"), new Utf8("54")).asJava))
    }
    "generate array for a Set of doubles" in {
      case class Test(set: Set[Double])
      val schema = AvroSchemaV2[Test]
      EncoderV2[Test].encode(Test(Set(1.2, 34.5, 54.3))) shouldBe ImmutableRecord(schema, Vector(Vector(1.2, 34.5, 54.3).asJava))
    }
    //    "support Seq[Tuple2] issue #156" in {
    //      case class TupleTest2(first: String, second: Seq[(TupleTestA, TupleTestB)])
    //      case class TupleTestA(parameter: Int)
    //      case class TupleTestB(parameter: Int)
    //      val schema = AvroSchema[TupleTest2]
    //      Encoder[TupleTest2].encode(TupleTest2("hello", Seq()), schema) shouldBe ImmutableRecord(schema, Vector(Vector(1.2, 34.5, 54.3).asJava))
    //    }
    //    "support Seq[Tuple3]" in {
    //      case class TupleTest3(first: String, second: Seq[(TupleTestA, TupleTestB, TupleTestC)])
    //      case class TupleTestA(parameter: Int)
    //      case class TupleTestB(parameter: Int)
    //      case class TupleTestC(parameter: Int)
    //      val schema = AvroSchema[TupleTest3]
    //      Encoder[TupleTest3].encode(Test(Set(1.2, 34.5, 54.3)), schema) shouldBe ImmutableRecord(schema, Vector(Vector(1.2, 34.5, 54.3).asJava))
    //    }
    //    "support Seq[Tuple4]" in {
    //      case class TupleTest4(first: String, second: Seq[(TupleTestA, TupleTestB, TupleTestC, TupleTestD)])
    //      case class TupleTestA(parameter: Int)
    //      case class TupleTestB(parameter: Int)
    //      case class TupleTestC(parameter: Int)
    //      case class TupleTestD(parameter: Int)
    //      val schema = AvroSchema[TupleTest4]
    //      Encoder[TupleTest3].encode(Test(Set(1.2, 34.5, 54.3)), schema) shouldBe ImmutableRecord(schema, Vector(Vector(1.2, 34.5, 54.3).asJava))
    //    }
    "support top level Seq[Double]" in {
      val schema = AvroSchemaV2[Array[Double]]
      EncoderV2[Array[Double]].encode(Array(1.2, 34.5, 54.3)) shouldBe new GenericData.Array[Double](schema, List(1.2, 34.5, 54.3).asJava)
    }
    "support top level List[Int]" in {
      val schema = AvroSchemaV2[List[Int]]
      EncoderV2[List[Int]].encode(List(1, 4, 9)) shouldBe new GenericData.Array[Int](schema, List(1, 4, 9).asJava)
    }
    "support top level Vector[String]" in {
      val schema = AvroSchemaV2[Vector[String]]
      EncoderV2[Vector[String]].encode(Vector("a", "z")) shouldBe new GenericData.Array[Utf8](schema, List(new Utf8("a"), new Utf8("z")).asJava)
    }
    "support top level Set[Boolean]" in {
      val schema = AvroSchemaV2[Set[Boolean]]
      EncoderV2[Set[Boolean]].encode(Set(true, false, true)) shouldBe new GenericData.Array[Boolean](schema, Set(true, false).asJava)
    }
  }
}

