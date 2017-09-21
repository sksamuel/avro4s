package com.sksamuel.avro4s

import java.io.File

import org.scalatest.{Matchers, WordSpec}

import scala.math.BigDecimal.RoundingMode.HALF_EVEN

case class Pizza(name: String, ingredients: Seq[Ingredient], vegetarian: Boolean, vegan: Boolean, calories: Int)
case class Ingredient(name: String, sugar: Double, fat: Double)

case class Product(name: String, price: Price, litres: BigDecimal)
case class Price(currency: String, amount: BigDecimal)

object Price {
  implicit val sp = ScaleAndPrecisionAndRoundingMode(2, 8, HALF_EVEN)
  implicit val schema = SchemaFor[Price]
}

object Product {
  implicit val sp = ScaleAndPrecisionAndRoundingMode(3, 8, HALF_EVEN)
  implicit val schema = SchemaFor[Product]
}

class ReadmeTest extends WordSpec with Matchers {

  "Readme" should {
    "exercise pizzas" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12.2, 4.4), Ingredient("onions", 1.2, 0.4)), false, false, 500)
      val hawaiian = Pizza("hawaiian", Seq(Ingredient("ham", 1.5, 5.6), Ingredient("pineapple", 5.2, 0.2)), false, false, 500)
      val file = new File("pizzas.avro")
      val os = AvroOutputStream.data[Pizza](file)
      os.write(Seq(pepperoni, hawaiian))
      os.flush()
      os.close()

      val is = AvroInputStream.data[Pizza](file)
      val pizzas = is.iterator.toList
      is.close()
      pizzas shouldEqual List(pepperoni, hawaiian)
      file.delete()
    }

    "exercise scoped scale/precision" in {
      Product.schema().toString(true) shouldEqual new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/scoped_implicits.avsc")).toString(true)
    }
  }
}
