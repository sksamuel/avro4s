//package com.sksamuel.avro4s
//
//import java.io.ByteArrayOutputStream
//
//import org.scalatest.{Matchers, WordSpec}
//
//class AvroBinaryOutputStreamTest extends WordSpec with Matchers {
//
//  val ennio = Composer("ennio morricone", "rome", Seq("legend of 1900", "ecstasy of gold"))
//
//  "AvroBinaryOutputStream" should {
//    "not include schema" in {
//      val baos = new ByteArrayOutputStream()
//      val output = AvroOutputStream.binary[Composer](baos)
//      output.write(ennio)
//      output.close()
//      new String(baos.toByteArray) should not include "birthplace"
//      new String(baos.toByteArray) should not include "compositions"
//    }
//  }
//}
