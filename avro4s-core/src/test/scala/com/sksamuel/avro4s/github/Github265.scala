package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{Decoder, Encoder, RecordFormat, SchemaFor}
import org.apache.avro.generic.GenericRecord
import org.scalatest.{FunSuite, Matchers}

import scala.language.higherKinds

class Github265 extends FunSuite with Matchers {

  case class Ingredient(name: String, sugar: Double, fat: Double)
  case class Pizza(name: String, ingredients: Seq[Ingredient], vegetarian: Boolean, vegan: Boolean, calories: Int)

  val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
  val hawaiian = Pizza("hawaiian", Seq(Ingredient("ham", 1.5, 5.6), Ingredient("pineapple", 5.2, 0.2)), false, false, 91)

  def toAvro[T: Encoder : Decoder : SchemaFor](obj: T): GenericRecord = RecordFormat[T].to(obj)

  val recordInAvro = toAvro[Pizza](pepperoni)
  printf(s"Avro of Pepperoni: $recordInAvro\n")
}
