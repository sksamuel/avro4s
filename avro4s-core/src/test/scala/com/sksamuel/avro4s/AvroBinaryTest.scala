package com.sksamuel.avro4s

import java.io.File

import org.scalatest.{Matchers, WordSpec}

class AvroBinaryTest extends WordSpec with Matchers {

  "AvroBinary" should {
    "be able to read its own output" in {

      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val file: File = new File("pizzas.avro.binary")

      val os = AvroOutputStream.binary[Pizza](file)
      os.write(pepperoni)
      os.close()

      val is = AvroInputStream.binary[Pizza](file)
      val pizzas = is.iterator.toList
      pizzas shouldBe List(pepperoni)
      is.close()

      file.delete()
    }
  }
}
