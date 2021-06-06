//package com.sksamuel.avro4s.streams.output
//
//import com.sksamuel.avro4s.record.encoder.FooValueType
//import org.apache.avro.util.Utf8
//
//class ValueTypeOutputStreamTest extends OutputStreamTest {
//
//  test("value type") {
//    case class Test(foo: FooValueType)
//    writeRead(Test(FooValueType("abc"))) { record =>
//      record.get("foo") shouldBe new Utf8("abc")
//    }
//  }
//
//  test("value types inside Options") {
//    case class Test(foo: Option[FooValueType])
//    writeRead(Test(Some(FooValueType("abc")))) { record =>
//      record.get("foo") shouldBe new Utf8("abc")
//    }
//    writeRead(Test(None)) { record =>
//      record.get("foo") shouldBe null
//    }
//  }
//}
