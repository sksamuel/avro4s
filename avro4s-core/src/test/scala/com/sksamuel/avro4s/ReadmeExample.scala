//package com.sksamuel.avro4s
//
//case class Pizza(name: String, ingredients: Seq[Ingredient], vegetarian: Boolean, vegan: Boolean, calories: Int)
//
//case class Ingredient(name: String, sugar: Double, fat: Double)
//
//object Test extends App {
//
//  import com.sksamuel.avro4s.ToSchema
//
//  val schema = ToSchema[Pizza].apply()
//  println(schema.toString(true))
//
//  import java.io.File
//  import com.sksamuel.avro4s.AvroOutputStream
//
//  val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12.2, 4.4), Ingredient("onions", 1.2, 0.4)), false, false, 500)
//  val hawaiian = Pizza("hawaiian", Seq(Ingredient("ham", 1.5, 5.6), Ingredient("pineapple", 5.2, 0.2)), false, false, 500)
//  val os = AvroOutputStream[Pizza](new File("pizzas.avro"))
//  os.write(Seq(pepperoni, hawaiian))
//  os.flush()
//  os.close()
//
//  import com.sksamuel.avro4s.AvroInputStream
//
//  val is = AvroInputStream[Pizza](new File("pizzas.avro"))
//  val pizzas = is.iterator.toSet
//  is.close()
//  println(pizzas.mkString("\n"))
//}
