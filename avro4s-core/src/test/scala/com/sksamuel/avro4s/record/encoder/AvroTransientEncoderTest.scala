//package com.sksamuel.avro4s.record.encoder
//
//import com.sksamuel.avro4s.{AvroTransient, Encoder, ImmutableRecord}
//import org.apache.avro.util.Utf8
//import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.matchers.should.Matchers
//
//class AvroTransientEncoderTest extends AnyFunSuite with Matchers {
//
//  test("encoder should skip @AvroTransient fields") {
//    case class Foo(a: String, @AvroTransient b: String, c: String)
//    val record = Encoder[Foo].encode(Foo("a", "b", "c")).asInstanceOf[ImmutableRecord]
//    record.values shouldBe Vector(new Utf8("a"), new Utf8("c"))
//  }
//}
