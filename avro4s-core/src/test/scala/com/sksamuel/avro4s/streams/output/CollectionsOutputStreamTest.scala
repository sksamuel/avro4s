package com.sksamuel.avro4s.streams.output

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8

class CollectionsOutputStreamTest extends OutputStreamTest {

  import scala.collection.JavaConverters._

  test("write Array of doubles") {
    case class Test(z: Array[Double])
    writeRead(Test(Array(234.4, 8741.3))) { record =>
      record.get("z").asInstanceOf[GenericData.Array[Double]].asScala shouldBe Seq(234.4, 8741.3)
    }
  }

  test("write Seq of doubles") {
    case class Test(z: Seq[Double])
    writeRead(Test(Seq(981.6, 23861.3))) { record =>
      record.get("z").asInstanceOf[GenericData.Array[Double]].asScala shouldBe Seq(981.6, 23861.3)
    }
  }

  test("write deep nested maps") {
    case class Test(z: Long)
    writeRead(Test(65653L)) { record =>
      record.get("z") shouldBe 65653L
    }
  }

  test("write Seq of nested classes") {
    case class Foo(a: String)
    case class Test(z: Seq[Foo])
    writeRead(Test(Seq(Foo("x"), Foo("y")))) { record =>
      val array = record.get("z").asInstanceOf[GenericData.Array[Int]].asScala
      array.head.asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("x")
      array.last.asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("y")
    }
  }

  test("write Set of doubles") {
    case class Test(z: Set[Double])
    writeRead(Test(Set(12.4, 6435.3))) { record =>
      record.get("z").asInstanceOf[GenericData.Array[Double]].asScala shouldBe Seq(12.4, 6435.3)
    }
  }

  test("write list of doubles") {
    case class Test(z: List[Double])
    writeRead(Test(List(234.234, 433.4, 345))) { record =>
      record.get("z").asInstanceOf[GenericData.Array[Double]].asScala shouldBe Seq(234.234, 433.4, 345)
    }
  }

  test("write list of booleans") {
    case class Test(z: List[Boolean])
    writeRead(Test(List(true, false, true))) { record =>
      record.get("z").asInstanceOf[GenericData.Array[Boolean]].asScala shouldBe Seq(true, false, true)
    }
  }

  test("write map of strings") {
    case class Test(z: Map[String, String])
    writeRead(Test(Map("a" -> "z", "c" -> "d"))) { record =>
      record.get("z").asInstanceOf[java.util.Map[_, _]].asScala shouldBe Map(new Utf8("a") -> new Utf8("z"), new Utf8("c") -> new Utf8("d"))
    }
  }

  test("write map of doubles") {
    case class Test(z: Map[String, Double])
    writeRead(Test(Map("a" -> 2914.4, "b" -> 92374.34))) { record =>
      record.get("z").asInstanceOf[java.util.Map[_, _]].asScala shouldBe Map(new Utf8("a") -> 2914.4, new Utf8("b") -> 92374.34)
    }
  }

  test("write map of booleans") {
    case class Test(z: Map[String, Boolean])
    writeRead(Test(Map("a" -> true, "b" -> false))) { record =>
      record.get("z").asInstanceOf[java.util.Map[_, _]].asScala shouldBe Map(new Utf8("a") -> true, new Utf8("b") -> false)
    }
  }

  test("write map of nested classes") {
    case class Foo(a: String)
    case class MapWithNestedClasses(z: Map[String, Foo])
    writeRead(MapWithNestedClasses(Map("foo" -> Foo("sam")))) { record =>
      val map = record.get("z").asInstanceOf[java.util.Map[Utf8, _]].asScala
      map(new Utf8("foo")).asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("sam")
    }
  }

  test("write Set of nested classes") {
    case class Foo(a: String)
    case class Test(z: Set[Foo])
    writeRead(Test(Set(Foo("a"), Foo("b")))) { record =>
      val set = record.get("z").asInstanceOf[GenericData.Array[GenericRecord]].asScala.toSet
      set.map(_.get("a")) shouldBe Set(new Utf8("a"), new Utf8("b"))
    }
  }

  test("write Vector of primitives as arrays") {
    case class Test(z: Vector[Int])
    writeRead(Test(Vector(1, 2, 5))) { record =>
      record.get("z").asInstanceOf[GenericData.Array[Int]].asScala shouldBe Seq(1, 2, 5)
    }
  }

  test("write Vector of nested classes") {
    case class Foo(a: String)
    case class Test(z: Vector[Foo])
    writeRead(Test(Vector(Foo("x"), Foo("y")))) { record =>
      val array = record.get("z").asInstanceOf[GenericData.Array[Int]].asScala
      array.head.asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("x")
      array.last.asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("y")
    }
  }
}