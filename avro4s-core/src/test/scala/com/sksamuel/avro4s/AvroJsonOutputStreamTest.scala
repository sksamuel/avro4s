package com.sksamuel.avro4s

import org.scalatest.{Matchers, WordSpec}

case class Rating(value: Int) extends AnyVal
case class Score(name: String, composer: String, rating: Rating)
case class Composer(name: String, birthplace: String, compositions: Seq[String])

class AvroJsonOutputStreamTest extends WordSpec with Matchers {

  val ennio = Composer("ennio morricone", "rome", Seq("legend of 1900", "ecstasy of gold"))
  val tgtbtu = Score("The good, the bad and the ugly", "ennio", Rating(10000))

  "AvroJsonOutputStream" should {
//    "produce json format" in {
//      val baos = new ByteArrayOutputStream()
//      val output = AvroJsonOutputStream[Composer](baos)
//      output.write(ennio)
//      output.close()
//      baos.toString("UTF-8") shouldBe "{\"name\":\"ennio morricone\",\"birthplace\":\"rome\",\"compositions\":[\"legend of 1900\",\"ecstasy of gold\"]}"
//    }
//    "support value classes" in {
//      val baos = new ByteArrayOutputStream()
//      val output = AvroJsonOutputStream[Score](baos)
//      output.write(tgtbtu)
//      output.close()
//      baos.toString("UTF-8") shouldBe """{"name":"The good, the bad and the ugly","composer":"ennio","rating":10000}"""
//    }
  }
}
