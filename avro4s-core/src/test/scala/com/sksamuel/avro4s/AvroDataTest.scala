//package com.sksamuel.avro4s
//
//import java.io.File
//
//import org.apache.avro.file.CodecFactory
//import org.scalatest.{Matchers, WordSpec}
//
//class AvroDataTest extends WordSpec with Matchers {
//  val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
//  val hawaiian = Pizza("hawaiian", Seq(Ingredient("ham", 1.5, 5.6), Ingredient("pineapple", 5.2, 0.2)), false, vegan = false, 91)
//
//  "AvroData" should {
//    "be able to read its own output" in {
//      val file: File = new File("pizzas.avro")
//      val os = AvroOutputStream.data[Pizza](file)
//      os.write(pepperoni)
//      os.write(hawaiian)
//      os.close()
//
//      val is = AvroInputStream.data[Pizza](file)
//      val pizzas = is.iterator.toList
//      pizzas shouldBe List(pepperoni, hawaiian)
//      is.close()
//      file.delete()
//    }
//
//    "be able to read its own output with codec" in {
//      val file: File = new File("pizzas.avro")
//      val os = AvroOutputStream.data[Pizza](file, CodecFactory.snappyCodec())
//      os.write(pepperoni)
//      os.write(hawaiian)
//      os.close()
//
//      val is = AvroInputStream.data[Pizza](file)
//      val pizzas = is.iterator.toList
//      pizzas shouldBe List(pepperoni, hawaiian)
//      is.close()
//      file.delete()
//    }
//
//    "be able to serialize/deserialize recursive data" in {
//      val data = Recursive(4, Some(Recursive(2, Some(Recursive(9, None)))))
//
//      val file: File = new File("recursive.avro")
//
//      // at the moment you need to store the recursive schema in a variable
//      // so that the compiler doesn't try to expand it forever
//      implicit val schema = SchemaFor[Recursive]
//
//      val os = AvroOutputStream.data[Recursive](file)
//      os.write(data)
//      os.close
//
//      val is = AvroInputStream.data[Recursive](file)
//      val parsed = is.iterator.toList
//      is.close()
//      file.delete()
//
//      parsed shouldBe List(data)
//    }
//  }
//}
