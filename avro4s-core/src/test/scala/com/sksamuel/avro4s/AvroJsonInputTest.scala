package com.sksamuel.avro4s

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class AvroJsonInputTest extends WordSpec with Matchers {

  case class Composer(name: String, birthplace: String, compositions: Seq[String])

  val ennio = Composer("ennio morricone", "rome", Seq("legend of 1900", "ecstasy of gold"))

  "AvroJsonInput" should {
    "serialise back to the case class as a set" in {
      val json = "{\"name\":\"ennio morricone\",\"birthplace\":\"rome\",\"compositions\":[\"legend of 1900\",\"ecstasy of gold\"]}"
      val in = new ByteInputStream(json.getBytes("UTF-8"), json.length)
      val input = new AvroJsonInputStream[Composer](in)
      val result = input.iterator.toSet
      result shouldBe Set(ennio)
    }

    "serialise back to a single entry" in {
      val json = "{\"name\":\"ennio morricone\",\"birthplace\":\"rome\",\"compositions\":[\"legend of 1900\",\"ecstasy of gold\"]}"
      val in = new ByteInputStream(json.getBytes("UTF-8"), json.length)
      val input = new AvroJsonInputStream[Composer](in)
      val result = input.singleEntity
      result shouldBe Success(ennio)
    }
  }

}
