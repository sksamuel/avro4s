//package com.sksamuel.avro4s.streams.input
//
//import com.sksamuel.avro4s.schema.{Colours, Wine}
//import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.matchers.should.Matchers
//
//class EnumInputStreamTest extends AnyFunSuite with Matchers with InputStreamTest {
//
//  case class JavaEnumTest(z: Wine)
//  case class OptionalJavaEnumTest(z: Option[Wine])
//  case class ScalaEnumTest(z: Colours.Value)
//  case class OptionalScalaEnumTest(z: Option[Colours.Value])
//
//  test("java enum") {
//    writeRead(JavaEnumTest(Wine.Malbec))
//  }
//
//  test("optional java enum") {
//    writeRead(OptionalJavaEnumTest(Some(Wine.Malbec)))
//    writeRead(OptionalJavaEnumTest(None))
//  }
//
//  test("scala enum") {
//    writeRead(ScalaEnumTest(Colours.Green))
//  }
//
//  test("optional scala enum") {
//    writeRead(OptionalScalaEnumTest(Some(Colours.Green)))
//    writeRead(OptionalScalaEnumTest(None))
//  }
//}
