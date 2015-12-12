package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import org.scalatest.{Matchers, WordSpec}

/**
  * An end to end test for a hugely nested structure.
  *
  * Currently tests:
  *
  * - Strings,
  * - Ints
  * - doubles
  * - booleans
  * - byte arrays
  * - seqs of case classes
  * - arrays of case classes
  * - Maps of Strings to Ints
  * -
  */
class UniverseTest extends WordSpec with Matchers {

  val g = Universe(
    factions = Seq(
      Faction("Imperial", true),
      Faction("Federation", true),
      Faction("Independant", false)
    ),
    manufacturers = Array(
      Manufacturer(
        name = "Gutamaya",
        ships = Seq(
          Ship(name = "Imperial Clipper", role = "fighter escort", maxSpeed = 430, jumpRange = 8.67, hardpoints = Map("medium" -> 4, "large" -> 2))
        )
      ),
      Manufacturer(
        name = "Core Dynamics",
        ships = Seq(
          Ship(name = "Eagle", role = "fighter", maxSpeed = 350, jumpRange = 15.4, hardpoints = Map("small" -> 3))
        )
      )
    )
  )

  "Avro4s" should {
    "support complex schema" in {
      val schema = AvroSchema2[Universe]
      println(schema.toString(true))
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/universe.avsc"))
      schema.toString(true) shouldBe expected.toString(true)
    }
    "support complex write" in {
      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Universe](output)
      avro.write(g)
      avro.close()
    }
  }
}

case class Universe(factions: Seq[Faction], manufacturers: Array[Manufacturer])

case class Faction(name: String, playable: Boolean)

case class Manufacturer(name: String, ships: Seq[Ship])

case class Ship(name: String, role: String, maxSpeed: Int, jumpRange: Double, hardpoints: Map[String, Int])

case class CQC(maps: Seq[PlayableMap])

case class PlayableMap()