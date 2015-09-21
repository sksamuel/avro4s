package com.sksamuel.avro4s

import org.scalatest.{Matchers, WordSpec}

class MacroWriterTest extends WordSpec with Matchers {

  import AvroImplicits._

  "SchemaGenerator.schemaFor" should {
    "generate correct schema" in {
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/gameofthrones.avsc"))
      val writer = schemaFor[GameOfThrones]
      println(writer.schema)
      writer.schema.toString(true) shouldBe expected.toString(true)
    }
  }
}

case class GameOfThrones(id: String,
                         kingdoms: Int,
                         rating: BigDecimal,
                         temperature: Double,
                         deathCount: Long,
                         aired: Boolean,
                         locations: Seq[String],
                         kings: Array[String],
                         seasons: List[Int],
                         alligence: Map[String, String],
                         throne: IronThrone,
                         houses: Seq[House])

case class IronThrone(swordCount: Int)

case class House(name: String, ruler: String)