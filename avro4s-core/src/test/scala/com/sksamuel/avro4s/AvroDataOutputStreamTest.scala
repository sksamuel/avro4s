package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import org.scalatest.{Matchers, WordSpec}

class AvroDataOutputStreamTest extends WordSpec with Matchers {

  case class Composer(name: String, birthplace: String, compositions: Seq[String])
  val ennio = Composer("ennio morricone", "rome", Seq("legend of 1900", "ecstasy of gold"))

  "AvroDataOutputStream" should {
    "not include schema" in {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.data[Composer](baos)
      output.write(ennio)
      output.close()
      new String(baos.toByteArray) should not include "birthplace"
      new String(baos.toByteArray) should not include "compositions"
    }
  }
}
