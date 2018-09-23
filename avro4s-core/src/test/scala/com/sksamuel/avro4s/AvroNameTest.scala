//package com.sksamuel.avro4s
//
//import org.scalatest.{Matchers, WordSpec}
//
//class AvroNameTest extends WordSpec with Matchers {
//
//  case class Foo(@AvroName("wibble") wobble: String, wubble: String)
//
//
//  "ToRecord" should {
//    "correctly be able to produce a record" in {
//      val toRecord = ToRecord[Foo]
//      toRecord(Foo("woop", "scoop"))
//    }
//  }
//
//  "FromRecord" should {
//    "correctly be able to produce a record" in {
//      val fromRecord = FromRecord[Foo]
//      val record = ToRecord[Foo](Foo("woop", "scoop"))
//
//      fromRecord(record)
//    }
//  }
//}
