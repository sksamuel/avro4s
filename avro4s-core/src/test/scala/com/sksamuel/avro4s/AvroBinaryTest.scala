//package com.sksamuel.avro4s
//
//import java.io.{ByteArrayOutputStream, File}
//
//import org.scalatest.{Matchers, WordSpec}
//
//case class Version1(string: String)
//case class Version2(string: String, int: Int = 3)
//
//class AvroBinaryTest extends WordSpec with Matchers {
//
//  val tgtbtu = Score("The good, the bad and the ugly", "ennio", Rating(10000))
//
//  "AvroBinary" should {
//    "be able to read its own output" in {
//
//      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
//
//      val file: File = new File("pizzas.avro.binary")
//
//      val os = AvroOutputStream.binary[Pizza](file)
//      os.write(pepperoni)
//      os.close()
//
//      val is = AvroInputStream.binary[Pizza](file)
//      val pizzas = is.iterator.toList
//      pizzas shouldBe List(pepperoni)
//      is.close()
//
//      file.delete()
//    }
//
//    "support value classes" in {
//      val baos = new ByteArrayOutputStream()
//      val output = AvroOutputStream.binary[Score](baos)
//      output.write(tgtbtu)
//      output.close()
//
//      val is = AvroInputStream.binary[Score](baos.toByteArray)
//      val pizzas = is.iterator.toList
//      pizzas shouldBe List(tgtbtu)
//      is.close()
//    }
//
//    "support schema evolution" in {
//      val v1 = Version1("hello")
//      val baos = new ByteArrayOutputStream()
//      val output = AvroOutputStream.binary[Version1](baos)
//      output.write(v1)
//      output.close()
//
//      val is = AvroInputStream.binary[Version2](baos.toByteArray, AvroSchema[Version1])
//      val v2 = is.iterator.toList.head
//      is.close()
//
//      v2.string shouldEqual v1.string
//      v2.int shouldEqual 3
//    }
//
//  }
//}
