package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.SchemaFor
import org.scalatest.{Matchers, WordSpec}
import shapeless.{:+:, CNil}

class UnionSchemaTest extends WordSpec with Matchers {
  "SchemaEncoder" should {
    "support unions and unions of unions" in {
      val single = SchemaFor[Union]()
      val unionOfUnions = SchemaFor[UnionOfUnions]()

      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/union.avsc"))

      single.toString(true) shouldBe expected.toString(true)
      unionOfUnions.toString(true) shouldBe expected.toString(true).replace("Union", "UnionOfUnions")
    }
  }
}

case class UnionOfUnions(union: (Int :+: String :+: CNil) :+: Boolean :+: CNil)
