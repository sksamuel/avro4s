package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.scalatest.{Matchers, WordSpec}

class MacroWriterTest extends WordSpec with Matchers {

  import SchemaGenerator._

  "SchemaGenerator.schemaFor" should {
    "generate correct schema" in {
      val expected = new Schema.Parser().parse(getClass.getResourceAsStream("/gameofthrones.avsc"))
      val writer = schemaFor[GameOfThrones]
      writer.schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class GameOfThrones(id: String,
                         season: Int,
                         rating: Double,
                         deathCount: Long,
                         aired: Boolean)