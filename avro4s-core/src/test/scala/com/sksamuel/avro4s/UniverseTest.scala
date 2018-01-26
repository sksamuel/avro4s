package com.sksamuel.avro4s

import java.io.FileOutputStream
import java.nio.file.Paths

import org.scalatest.{Matchers, WordSpec}

/**
  * An end to end test for a hugely nested structure.
  *
  * Currently tests:
  *
  * - Strings,
  * - ints
  * - longs
  * - doubles
  * - booleans
  * - big decimals
  * - seqs of Strings
  * - seqs of case classes
  * - lists of case classes
  * - Maps of Strings to Ints
  * - Maps of Strings to Case Classes
  * - Sets of Strings
  * - Options of Strings
  * - Options of Case classes
  * - Options of enumerations
  * - Either[A,B] where A and B are both case classes
  * - Either[A,B] where A and B are both primitives
  */
class UniverseTest extends WordSpec with Matchers {

  val clipper = Ship(name = "Imperial Clipper", role = "fighter escort", maxSpeed = 430, jumpRange = 8.67, hardpoints = Map(("medium", 4), ("large", 2)), defaultWeapon = Some("pulse laser"))
  val eagle = Ship(name = "Eagle", role = "fighter", maxSpeed = 350, jumpRange = 15.4, hardpoints = Map(("small", 3)), defaultWeapon = None)
  val earth = Planet("Earth", "Sol")
  val mars = Planet("Mars", "Sol", Some(PlanetClass.H))

  val g = Universe(
    factions = Seq(
      Faction("Imperial", true, homeworld = Option(earth), shipRanks = Map(("baron", clipper)), area = 4461244.55),
      Faction("Federation", true, homeworld = Option(mars), area = 3969244.18),
      Faction("Independant", false, homeworld = None, area = 15662.18)
    ),
    rankings = Seq("harmless", "competent", "deadly", "dangerous", "elite"),
    nebulae = Set("horsehead", "orion", "barnards loop"),
    manufacturers = List(
      Manufacturer(
        name = "Gutamaya",
        ships = Seq(clipper)
      ),
      Manufacturer(
        name = "Core Dynamics",
        ships = Seq(eagle)
      )
    ),
    cqc = CQC(
      maps = Seq(
        PlayableMap(name = "level1", bonus = Left("weapon"), stationOrPlanet = Left(Station("orbis"))),
        PlayableMap(name = "level2", bonus = Right(123l), stationOrPlanet = Right(earth))
      )
    )
  )

  "Avro4s" should {
    "support complex schema" in {
    //  val schema = SchemaFor[Universe].apply()
    //  val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/universe.avsc"))
    //  schema.toString(true) shouldBe expected.toString(true)
    }
    "support complex write" in {
      val output = new FileOutputStream("universe.avro")
      val avro = AvroOutputStream.data[Universe](output)
      avro.write(g)
      avro.close()
    }
    "support complex read" in {
      val avro = AvroInputStream.data[Universe](Paths.get("universe.avro"))
      val universe = avro.iterator.next()
      universe shouldBe g
    }
  }
}

case class Universe(factions: Seq[Faction], rankings: Seq[String], manufacturers: List[Manufacturer], cqc: CQC, nebulae: Set[String])

case class Faction(name: String, playable: Boolean, homeworld: Option[Planet], shipRanks: Map[String, Ship] = Map.empty, area: BigDecimal)

object PlanetClass extends Enumeration
{
  val H, L, M = Value
}

case class Planet(name: String, system: String, planetClass: Option[PlanetClass.Value] = None)

case class Station(name: String)

case class Manufacturer(name: String, ships: Seq[Ship])

case class Ship(name: String, role: String, maxSpeed: Int, jumpRange: Double, hardpoints: Map[String, Int], defaultWeapon: Option[String])

case class CQC(maps: Seq[PlayableMap])

case class PlayableMap(name: String, bonus: Either[String, Long], stationOrPlanet: Either[Station, Planet])