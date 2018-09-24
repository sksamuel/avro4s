package com.sksamuel.avro4s.streams.output

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8

class CollectionsDataOutputStreamTest extends DataOutputStreamTest {

  import scala.collection.JavaConverters._

  test("write Array of doubles") {
    case class Test(z: Array[Double])
    val out = write(Test(Array(234.4, 8741.3)))
    val record = read[Test](out)
    record.get("z").asInstanceOf[GenericData.Array[Double]].asScala shouldBe Seq(234.4, 8741.3)
  }

  test("write Seq of doubles") {
    case class Test(z: Seq[Double])
    val out = write(Test(Seq(981.6, 23861.3)))
    val record = read[Test](out)
    record.get("z").asInstanceOf[GenericData.Array[Double]].asScala shouldBe Seq(981.6, 23861.3)
  }

  test("write deep nested maps") {
    case class Test(z: Long)
    val out = write(Test(65653L))
    val record = read[Test](out)
    record.get("z") shouldBe 65653L
  }

  test("write Seq of nested classes") {
    case class Foo(a: String)
    case class Test(z: Seq[Foo])
    val out = write(Test(Seq(Foo("x"), Foo("y"))))
    val record = read[Test](out)
    val array = record.get("z").asInstanceOf[GenericData.Array[Int]].asScala
    array.head.asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("x")
    array.last.asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("y")
  }

  test("write Set of doubles") {
    case class Test(z: Set[Double])
    val out = write(Test(Set(12.4, 6435.3)))
    val record = read[Test](out)
    record.get("z").asInstanceOf[GenericData.Array[Double]].asScala shouldBe Seq(12.4, 6435.3)
  }

  test("write list of doubles") {
    case class Test(z: List[Double])
    val out = write(Test(List(234.234, 433.4, 345)))
    val record = read[Test](out)
    record.get("z").asInstanceOf[GenericData.Array[Double]].asScala shouldBe Seq(234.234, 433.4, 345)
  }

  test("write list of booleans") {
    case class Test(z: List[Boolean])
    val out = write(Test(List(true, false, true)))
    val record = read[Test](out)
    record.get("z").asInstanceOf[GenericData.Array[Boolean]].asScala shouldBe Seq(true, false, true)
  }

  test("write map of strings") {
    case class Test(z: Map[String, String])
    val out = write(Test(Map("a" -> "z", "c" -> "d")))
    val record = read[Test](out)
    record.get("z").asInstanceOf[java.util.Map[_, _]].asScala shouldBe Map(new Utf8("a") -> new Utf8("z"), new Utf8("c") -> new Utf8("d"))
  }

  test("write map of doubles") {
    case class Test(z: Map[String, Double])
    val out = write(Test(Map("a" -> 2914.4, "b" -> 92374.34)))
    val record = read[Test](out)
    record.get("z").asInstanceOf[java.util.Map[_, _]].asScala shouldBe Map(new Utf8("a") -> 2914.4, new Utf8("b") -> 92374.34)
  }

  test("write map of booleans") {
    case class Test(z: Map[String, Boolean])
    val out = write(Test(Map("a" -> true, "b" -> false)))
    val record = read[Test](out)
    record.get("z").asInstanceOf[java.util.Map[_, _]].asScala shouldBe Map(new Utf8("a") -> true, new Utf8("b") -> false)
  }

  test("write map of nested classes") {
    case class Foo(a: String)
    case class MapWithNestedClasses(z: Map[String, Foo])
    val out = write(MapWithNestedClasses(Map("foo" -> Foo("sam"))))
    val record = read[MapWithNestedClasses](out)
    val map = record.get("z").asInstanceOf[java.util.Map[Utf8, _]].asScala
    map(new Utf8("foo")).asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("sam")
  }

  test("write Set of nested classes") {
    case class Foo(a: String)
    case class Test(z: Set[Foo])
    val out = write(Test(Set(Foo("a"), Foo("b"))))
    val record = read[Test](out)
    val array = record.get("z").asInstanceOf[GenericData.Array[Int]].asScala
    array.head.asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("a")
    array.last.asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("b")
  }

  test("write Vector of primitives as arrays") {
    case class Test(z: Vector[Int])
    val out = write(Test(Vector(1, 2, 5)))
    val record = read[Test](out)
    record.get("z").asInstanceOf[GenericData.Array[Int]].asScala shouldBe Seq(1, 2, 5)
  }

  test("write Vector of nested classes") {
    case class Foo(a: String)
    case class Test(z: Vector[Foo])
    val out = write(Test(Vector(Foo("x"), Foo("y"))))
    val record = read[Test](out)
    val array = record.get("z").asInstanceOf[GenericData.Array[Int]].asScala
    array.head.asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("x")
    array.last.asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("y")
  }
}