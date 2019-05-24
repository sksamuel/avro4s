package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{DefaultNamingStrategy, SchemaFor}
import org.scalatest.{FunSuite, Matchers}

sealed trait InnerTrait295
case class InnerTraitConcrete295(v: Int) extends InnerTrait295
case class InnerTraitConcrete295_2(v: Int) extends InnerTrait295

sealed trait OuterTrait295
case class OuterConcrete295(inner: InnerTrait295) extends OuterTrait295

class Github295 extends FunSuite with Matchers {
  test("Cannot generate schema for a sealed trait, which concrete case class has another sealed trait as an argument #295") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/github295.json"))
    SchemaFor[OuterTrait295].schema.toString(true) shouldBe expected.toString(true)
  }
}
