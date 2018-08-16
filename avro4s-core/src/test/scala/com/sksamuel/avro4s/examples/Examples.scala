//package com.sksamuel.avro4s.examples
//
//import java.io.ByteArrayOutputStream
//
//import org.scalatest.{Matchers, WordSpec}
//
//class Examples extends WordSpec with Matchers {
//  import com.sksamuel.avro4s.AvroOutputStream
//
//  case class Ingredient(name: String, sugar: Double, fat: Double)
//  case class Pizza(name: String, ingredients: Seq[Ingredient], vegetarian: Boolean, vegan: Boolean, calories: Int)
//
//  val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
//  val hawaiian = Pizza("hawaiian", Seq(Ingredient("ham", 1.5, 5.6), Ingredient("pineapple", 5.2, 0.2)), false, false, 91)
//
//  "AvroStream" should {
//
//    "read back the objects " in {
//      val bos = new ByteArrayOutputStream()
//
//      val os = AvroOutputStream.data[Pizza](bos)
//      os.write(Seq(pepperoni, hawaiian))
//      os.flush()
//      os.close()
//
//      import com.sksamuel.avro4s.AvroInputStream
//
//      val is = AvroInputStream.data[Pizza](bos.toByteArray)
//      val pizzas = is.iterator.toSeq
//      is.close()
//
//      pizzas shouldBe Seq(pepperoni, hawaiian)
//    }
//  }
//
//}
