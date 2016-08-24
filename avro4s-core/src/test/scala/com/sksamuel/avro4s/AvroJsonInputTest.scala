package com.sksamuel.avro4s

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class AvroJsonInputTest extends WordSpec with Matchers {

  val ennio = Composer("ennio morricone", "rome", Seq("legend of 1900", "ecstasy of gold"))
  val tgtbtu = Score("The good, the bad and the ugly", "ennio", Rating(10000))

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
    "support value classes" in {
      val json = """{"name":"The good, the bad and the ugly","composer":"ennio","rating":10000}"""
      val in = new ByteInputStream(json.getBytes("UTF-8"), json.length)
      val input = new AvroJsonInputStream[Score](in)
      val result = input.singleEntity
      result shouldBe Success(tgtbtu)
    }
  }

}
