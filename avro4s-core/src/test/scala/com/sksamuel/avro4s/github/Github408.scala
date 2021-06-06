//package com.sksamuel.avro4s.github
//
//import com.sksamuel.avro4s.streams.input.InputStreamTest
//
//case class Woo(b: Boolean)
//case class OptionOfSeqOfCaseClass(ws: Option[Seq[Woo]])
//
//class Github408 extends InputStreamTest {
//
//  test("round trip of Option[Seq[CaseClass]]") {
//    writeRead(OptionOfSeqOfCaseClass(Some(List(Woo(true), Woo(false), Woo(true)))))
//  }
//}
