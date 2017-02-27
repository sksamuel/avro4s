package com.sksamuel.avro4s

import java.io.{ByteArrayOutputStream, File}

import org.scalatest.{Matchers, WordSpec}

class AvroBinaryTest extends WordSpec with Matchers {

  val tgtbtu = Score("The good, the bad and the ugly", "ennio", Rating(10000))

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

    "support value classes" in {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Score](baos)
      output.write(tgtbtu)
      output.close()

      val is = AvroInputStream.binary[Score](baos.toByteArray)
      val pizzas = is.iterator.toList
      pizzas shouldBe List(tgtbtu)
      is.close()
    }

    "support string type" in {
      val testSource = "hello"
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[String](baos)
      output.write(testSource)
      output.close()

      val is = AvroInputStream.binary[String](baos.toByteArray)
      val pizzas = is.iterator.toList
      pizzas shouldBe List(testSource)
      is.close()
    }

    "support int type" in {
      val testSource = 123
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Int](baos)
      output.write(testSource)
      output.close()

      val is = AvroInputStream.binary[Int](baos.toByteArray)
      val pizzas = is.iterator.toList
      pizzas shouldBe List(testSource)
      is.close()
    }

    "support long type" in {
      val testSource = 123L
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Long](baos)
      output.write(testSource)
      output.close()

      val is = AvroInputStream.binary[Long](baos.toByteArray)
      val pizzas = is.iterator.toList
      pizzas shouldBe List(testSource)
      is.close()
    }

    "support double type" in {
      val testSource: Double = 123.123
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Double](baos)
      output.write(testSource)
      output.close()

      val is = AvroInputStream.binary[Double](baos.toByteArray)
      val pizzas = is.iterator.toList
      pizzas shouldBe List(testSource)
      is.close()
    }

    "support float type" in {
      val testSource: Float = 123.123F
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Float](baos)
      output.write(testSource)
      output.close()

      val is = AvroInputStream.binary[Float](baos.toByteArray)
      val pizzas = is.iterator.toList
      pizzas shouldBe List(testSource)
      is.close()
    }

  }
}
