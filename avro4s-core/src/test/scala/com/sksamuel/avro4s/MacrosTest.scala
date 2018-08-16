//package com.sksamuel.avro4s
//
//import org.scalatest.{Matchers, WordSpec}
//
//class MacrosTest extends WordSpec with Matchers {
//
//  case class Ingredient(name: String, sugar: Double, fat: Double)
//  case class Pizza(name: String, ingredients: Seq[Ingredient], vegetarian: Boolean, vegan: Boolean, calories: Int)
//
//  val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
//
//  "SchemaFor" should {
//    "should return the same schema instance" in {
//      val schemaFor = SchemaFor[Pizza]
//      val s1 = schemaFor()
//      val s2 = schemaFor()
//
//      (s1 eq s2) shouldBe true
//    }
//  }
//
//  "ToRecord" should {
//    "should return the same schema instance" in {
//      val toRecord = ToRecord[Pizza]
//
//      val rec1 = toRecord(pepperoni)
//      val rec2 = toRecord(pepperoni)
//
//      (rec1.getSchema eq rec2.getSchema) shouldBe true
//    }
//  }
//
//  "FromRecord" should {
//    "should convert to same instance" in {
//      val toRecord = ToRecord[Pizza]
//      val rec1 = toRecord(pepperoni)
//      val fromRecord = FromRecord[Pizza]
//
//      val pepperoni2 = fromRecord(rec1)
//
//      pepperoni2 shouldBe pepperoni
//    }
//  }
//}
