package com.sksamuel.avro4s.streams.input

class CollectionsInputStreamTest extends InputStreamTest {

  case class FooString(a: String)

  case class ArrayDoubles(z: Array[Double])

  case class ListDoubles(z: List[Double])
  case class ListBooleans(z: List[Boolean])

  case class SeqDoubles(z: Seq[Double])
  case class SeqFoo(z: Seq[FooString])

  case class SetDoubles(z: Set[Double])
  case class SetFoo(z: Set[FooString])

  case class MapStrings(z: Map[String, String])
  case class MapDoubles(z: Map[String, Double])
  case class MapBooleans(z: Map[String, Boolean])
  case class MapWithNestedClasses(z: Map[String, FooString])

  case class VectorInt(z: Vector[Int])
  case class VectorFoo(z: Vector[FooString])

  test("read write Array of doubles") {
    val x = ArrayDoubles(Array(234.4, 8741.3))
    val out1 = writeData(x)
    readData[ArrayDoubles](out1).z.toList shouldBe x.z.toList
    val out2 = writeBinary(x)
    readBinary[ArrayDoubles](out2).z.toList shouldBe x.z.toList
  }

  test("read write Seq of doubles") {
    writeRead(SeqDoubles(Seq(981.6, 23861.3)))
  }

  test("read write Seq of nested classes") {
    writeRead(SeqFoo(Seq(FooString("x"), FooString("y"))))
  }

  test("read write Set of doubles") {
    writeRead(SetDoubles(Set(12.4, 6435.3)))
  }

  test("read write list of doubles") {
    writeRead(ListDoubles(List(234.234, 433.4, 345)))
  }

  test("read write list of booleans") {
    writeRead(ListBooleans(List(true, false, true)))
  }

  test("read write map of strings") {
    writeRead(MapStrings(Map("a" -> "z", "c" -> "d")))
  }

  test("read write map of doubles") {
    writeRead(MapDoubles(Map("a" -> 2914.4, "b" -> 92374.34)))
  }

  test("read write map of booleans") {
    writeRead(MapBooleans(Map("a" -> true, "b" -> false)))
  }

  test("read write map of nested classes") {
    writeRead(MapWithNestedClasses(Map("foo" -> FooString("sam"))))
  }

  test("read write Set of nested classes") {
    writeRead(SetFoo(Set(FooString("a"), FooString("b"))))
  }

  test("read write Vector") {
    writeRead(VectorInt(Vector(1, 2, 5)))
  }

  test("read write Vector of nested classes") {
    writeRead(VectorFoo(Vector(FooString("x"), FooString("y"))))
  }
}