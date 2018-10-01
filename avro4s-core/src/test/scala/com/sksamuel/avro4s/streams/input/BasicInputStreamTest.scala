package com.sksamuel.avro4s.streams.input

class BasicInputStreamTest extends InputStreamTest {

  case class BooleanTest(z: Boolean)
  case class StringTest(z: String)
  case class FloatTest(z: Float)
  case class DoubleTest(z: Double)
  case class IntTest(z: Int)
  case class LongTest(z: Long)

  test("read write out booleans") {
    writeRead(BooleanTest(true))
  }

  test("read write out strings") {
    writeRead(StringTest("Hello world"))
  }

  test("read write out longs") {
    writeRead(LongTest(65653L))
  }

  test("read write out ints") {
    writeRead(IntTest(44))
  }

  test("read write out doubles") {
    writeRead(DoubleTest(3.235))
  }

  test("read write out floats") {
    writeRead(FloatTest(3.4F))
  }
}