package com.sksamuel.avro4s.streams.output

import org.apache.avro.util.Utf8

class BasicDataOutputStreamTest extends DataOutputStreamTest {

  test("write out booleans") {
    case class Test(z: Boolean)
    val out = write(Test(true))
    val record = read[Test](out)
    record.get("z") shouldBe true
  }

  test("write out strings") {
    case class Test(z: String)
    val out = write(Test("Hello world"))
    val record = read[Test](out)
    record.get("z") shouldBe new Utf8("Hello world")
  }

  test("write out longs") {
    case class Test(z: Long)
    val out = write(Test(65653L))
    val record = read[Test](out)
    record.get("z") shouldBe 65653L
  }

  test("write out ints") {
    case class Test(z: Int)
    val out = write(Test(44))
    val record = read[Test](out)
    record.get("z") shouldBe 44
  }

  test("write out doubles") {
    case class Test(z: Double)
    val out = write(Test(3.235))
    val record = read[Test](out)
    record.get("z") shouldBe 3.235
  }

  test("write out floats") {
    case class Test(z: Float)
    val out = write(Test(3.4F))
    val record = read[Test](out)
    record.get("z") shouldBe 3.4F
  }
}