package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{Encoder, InternalRecord, SchemaEncoder}
import org.scalatest.{Matchers, WordSpec}

class ArrayEncoderTest extends WordSpec with Matchers {

  "Encoder" should {
    "generate array for a vector of primitives" in {
      case class Test(booleans: Vector[Boolean])
      val schema = SchemaEncoder[Test].encode()
      Encoder[Test].encode(Test(Vector(true, false, true)), schema).asInstanceOf[InternalRecord].values.head.asInstanceOf[Array[_]].toVector shouldBe Vector(true, false, true)
    }
    "generate array for an vector of records" in {
      case class Test(records: Vector[Record])
      case class Record(str: String, double: Double)
      val schema = SchemaEncoder[Test].encode()
      val record = Encoder[Test].encode(Test(Vector(Record("abc", 12.34))), schema).asInstanceOf[InternalRecord]
      val nested = record.values.head.asInstanceOf[Array[AnyRef]]
      nested.head.asInstanceOf[InternalRecord].values.head shouldBe "abc"
      nested.head.asInstanceOf[InternalRecord].values.last shouldBe 12.34
    }
    "generate array for a scala.collection.immutable.Seq of primitives" in {
      case class Test(seq: Seq[String])
      val schema = SchemaEncoder[Test].encode()
      Encoder[Test].encode(Test(Vector("a", "34", "fgD")), schema).asInstanceOf[InternalRecord].values.head.asInstanceOf[Array[_]].toVector shouldBe Vector("a", "34", "fgD")
    }
    "generate array for an Array of primitives" in {
      case class Test(array: Array[Boolean])
      val schema = SchemaEncoder[Test].encode()
      Encoder[Test].encode(Test(Array(true, false, true)), schema).asInstanceOf[InternalRecord].values.head.asInstanceOf[Array[_]].toVector shouldBe Vector(true, false, true)
    }
    "generate array for a List of primitives" in {
      case class Test(list: List[String])
      val schema = SchemaEncoder[Test].encode()
      Encoder[Test].encode(Test(List("qwe", "we23", "54")), schema).asInstanceOf[InternalRecord].values.head.asInstanceOf[Array[_]].toVector shouldBe Vector("qwe", "we23", "54")
    }
    "generate array for a scala.collection.immutable.Seq of records" in {
      case class Nested(goo: String)
      case class Test(seq: Seq[Nested])
      val schema = SchemaEncoder[Test].encode()
    }
    "generate array for an Array of records" in {
      case class Nested(goo: String)
      case class Test(array: Array[Nested])
      val schema = SchemaEncoder[Test].encode()
    }
    "generate array for a List of records" in {
      case class Nested(goo: String)
      case class Test(list: List[Nested])
      val schema = SchemaEncoder[Test].encode()
    }
    "generate array for a Set of records" in {
      case class Nested(goo: String)
      case class NestedSet(set: Set[Nested])
      val schema = SchemaEncoder[NestedSet].encode()
    }
    "generate array for a Set of strings" in {
      case class Test(set: Set[String])
      val schema = SchemaEncoder[Test].encode()
      Encoder[Test].encode(Test(Set("qwe", "we23", "54")), schema).asInstanceOf[InternalRecord].values.head.asInstanceOf[Array[_]].toVector shouldBe Vector("qwe", "we23", "54")
    }
    "generate array for a Set of doubles" in {
      case class Test(set: Set[Double])
      val schema = SchemaEncoder[Test].encode()
      Encoder[Test].encode(Test(Set(34.45, 66.7, 43.34)), schema).asInstanceOf[InternalRecord].values.head.asInstanceOf[Array[_]].toVector shouldBe Vector(34.45, 66.7, 43.34)
    }
    //    "support Seq[Tuple2] issue #156" in {
    //      val schema = SchemaEncoder[TupleTest2].encode()
    //    }
    //    "support Seq[Tuple3]" in {
    //      val schema = SchemaEncoder[TupleTest3].encode()
    //    }
  }
}

case class TupleTest2(first: String, second: Seq[(TupleTestA, TupleTestB)])
case class TupleTest3(first: String, second: Seq[(TupleTestA, TupleTestB, TupleTestC)])
case class TupleTestA(parameter: Int)
case class TupleTestB(parameter: Int)
case class TupleTestC(parameter: Int)