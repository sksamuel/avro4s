package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class AvroJsonInputTest extends WordSpec with Matchers {

  case class Composer(name: String, birthplace: String, compositions: Seq[String])

  val ennio = Composer("ennio morricone", "rome", Seq("legend of 1900", "ecstasy of gold"))

  "AvroJsonInput" should {
    "serialise back to the case class as a set" in {
      val baos = new ByteArrayOutputStream()
      val output = AvroJsonOutput[Composer](baos)
      output.write(ennio)
      output.close()
      baos.toString("UTF-8") shouldBe "{\"name\":\"ennio morricone\",\"birthplace\":\"rome\",\"compositions\":[\"legend of 1900\",\"ecstasy of gold\"]}"
      val in = new ByteInputStream(baos.toByteArray, baos.size())
      val input = new AvroJsonInputStream[Composer](in)
      val result = input.iterator.toSet
      result shouldBe Set(ennio)
    }

    "serialise back to a single entry" in {
      val baos = new ByteArrayOutputStream()
      val output = AvroJsonOutput[Composer](baos)
      output.write(ennio)
      output.close()
      baos.toString("UTF-8") shouldBe "{\"name\":\"ennio morricone\",\"birthplace\":\"rome\",\"compositions\":[\"legend of 1900\",\"ecstasy of gold\"]}"
      val in = new ByteInputStream(baos.toByteArray, baos.size())
      val input = new AvroJsonInputStream[Composer](in)
      val result = input.singleEntity
      result shouldBe Success(ennio)
    }
  }

}
