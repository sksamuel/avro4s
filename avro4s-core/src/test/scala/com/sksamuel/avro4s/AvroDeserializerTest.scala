package com.sksamuel.avro4s

import java.io.File

import org.scalatest.concurrent.Timeouts
import org.scalatest.{Matchers, WordSpec}

class AvroDeserializerTest extends WordSpec with Matchers with Timeouts {

  val michelangelo = Artist("michelangelo", 1475, 1564, "Caprese", Seq("sculpture", "fresco"))
  val raphael = Artist("raphael", 1483, 1520, "florence", Seq("painter", "architect"))

  "AvroDeserializer" should {
    "read simple records" in {
      implicit val s = AvroImplicits.schemaFor[Artist]
      implicit val r = AvroImplicits.populatorFor[Artist]
      val file = new File(getClass.getResource("/painters.avro").getFile)
      val in = AvroInputStream[Artist](file)
      val painters = in.iterator.toSet
      painters shouldBe Set(michelangelo, raphael)
      in.close()
    }
  }

  implicit val s = AvroImplicits.schemaFor[Artist]
  println(s.schema)
}