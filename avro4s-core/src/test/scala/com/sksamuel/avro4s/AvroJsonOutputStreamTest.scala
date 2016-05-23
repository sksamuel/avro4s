package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import org.scalatest.{Matchers, WordSpec}

class AvroJsonOutputStreamTest extends WordSpec with Matchers {

  case class Composer(name: String, birthplace: String, compositions: Seq[String])

  val ennio = Composer("ennio morricone", "rome", Seq("legend of 1900", "ecstasy of gold"))

  "AvroJsonOutputStream" should {
    "produce json format" in {
      val baos = new ByteArrayOutputStream()
      val output = AvroJsonOutputStream[Composer](baos)
      output.write(ennio)
      output.close()
      baos.toString("UTF-8") shouldBe "{\"name\":\"ennio morricone\",\"birthplace\":\"rome\",\"compositions\":[\"legend of 1900\",\"ecstasy of gold\"]}"
    }
  }
}
