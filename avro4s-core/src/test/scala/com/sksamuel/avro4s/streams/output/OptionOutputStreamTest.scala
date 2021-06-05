//package com.sksamuel.avro4s.streams.output
//
//import org.apache.avro.generic.GenericRecord
//import org.apache.avro.util.Utf8
//
//class OptionOutputStreamTest extends OutputStreamTest {
//
//  test("options of booleans") {
//    case class Test(z: Option[Boolean])
//    writeRead(Test(Some(true))) { record =>
//      record.get("z") shouldBe true
//    }
//    writeRead(Test(None)) { record =>
//      record.get("z") shouldBe null
//    }
//  }
//
//  test("options of ints") {
//    case class Test(z: Option[Int])
//    writeRead(Test(Some(43242))) { record =>
//      record.get("z") shouldBe 43242
//    }
//    writeRead(Test(None)) { record =>
//      record.get("z") shouldBe null
//    }
//  }
//
//  test("options of longs") {
//    case class Test(z: Option[Long])
//    writeRead(Test(Some(43242L))) { record =>
//      record.get("z") shouldBe 43242L
//    }
//    writeRead(Test(None)) { record =>
//      record.get("z") shouldBe null
//    }
//  }
//
//  test("options of doubles") {
//    case class Test(z: Option[Double])
//    writeRead(Test(Some(123.34))) { record =>
//      record.get("z") shouldBe java.lang.Double.valueOf(123.34)
//    }
//    writeRead(Test(None)) { record =>
//      record.get("z") shouldBe null
//    }
//  }
//
//  test("options of strings") {
//    case class Test(z: Option[String])
//    writeRead(Test(Some("hello"))) { record =>
//      record.get("z") shouldBe new Utf8("hello")
//    }
//    writeRead(Test(None)) { record =>
//      record.get("z") shouldBe null
//    }
//  }
//
//  test("options of classes") {
//    case class Foo(s: String)
//    case class Test(z: Option[Foo])
//    writeRead(Test(Some(Foo("hello")))) { record =>
//      record.get("z").asInstanceOf[GenericRecord].get("s") shouldBe new Utf8("hello")
//    }
//    writeRead(Test(None)) { record =>
//      record.get("z") shouldBe null
//    }
//  }
//}