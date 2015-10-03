package com.sksamuel.avro4s

import java.io.File

import org.scalatest.concurrent.Timeouts
import org.scalatest.{Matchers, WordSpec}

class AvroInputStreamTest extends WordSpec with Matchers with Timeouts {

  val michelangelo = Artist("michelangelo", 1475, 1564, "Caprese", Seq("sculpture", "fresco"))
  val raphael = Artist("raphael", 1483, 1520, "florence", Seq("painter", "architect"))

  import AvroImplicits._

  "AvroDeserializer" should {
    "read simple records" in {
      val file = new File(this.getClass.getResource("/painters.avro").getFile)
      val in = AvroInputStream[Artist](file)
      val painters = in.iterator.toSet
      painters shouldBe Set(michelangelo, raphael)
      in.close()
    }
    "populate to Options" in {
      val options = Seq(OptionReadTest(Option("sammy")), OptionReadTest(None))
      //      val out = AvroOutputStream[OptionReadTest](new File("options.avro"))
      //      out.write(options)
      //      out.close()
      val in = AvroInputStream[OptionReadTest](getClass.getResource("/options.avro").getFile)
      in.iterator.toSet shouldBe options.toSet
      in.close()
    }
  }
}

case class OptionReadTest(option: Option[String])