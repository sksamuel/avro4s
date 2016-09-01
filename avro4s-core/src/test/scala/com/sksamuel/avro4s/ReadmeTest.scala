package com.sksamuel.avro4s

import java.io.File
import org.scalatest.{Matchers, WordSpec}

case class Pizza(name: String, ingredients: Seq[Ingredient], vegetarian: Boolean, vegan: Boolean, calories: Int)
case class Ingredient(name: String, sugar: Double, fat: Double)

case class Product(name: String, price: Price, litres: BigDecimal)
case class Price(currency: String, amount: BigDecimal)

object Price {
  implicit val sp = ScaleAndPrecision(10,2)
  implicit val schema = SchemaFor[Price]
}

object Product {
  implicit val sp = ScaleAndPrecision(8,4)
  implicit val schema = SchemaFor[Product]
}

class ReadmeTest extends WordSpec with Matchers {

  "Readme" should {
    "exercise pizzas" in {
      val schema = SchemaFor[Pizza]()

      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12.2, 4.4), Ingredient("onions", 1.2, 0.4)), false, false, 500)
      val hawaiian = Pizza("hawaiian", Seq(Ingredient("ham", 1.5, 5.6), Ingredient("pineapple", 5.2, 0.2)), false, false, 500)
      val os = AvroOutputStream[Pizza](new File("pizzas.avro"))
      os.write(Seq(pepperoni, hawaiian))
      os.flush()
      os.close()

      val is = AvroInputStream[Pizza](new File("pizzas.avro"))
      val pizzas = is.iterator.toList
      is.close()
      pizzas shouldEqual List(pepperoni, hawaiian)
    }
  }
}
