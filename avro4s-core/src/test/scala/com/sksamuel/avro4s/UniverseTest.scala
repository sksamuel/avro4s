//package com.sksamuel.avro4s
//
//import java.io.FileOutputStream
//import java.nio.file.Paths
//
//import org.scalatest.{Matchers, WordSpec}
//
///**
//  * An end to end test for a hugely nested structure.
//  *
//  * Currently tests:
//  *
//  * - Strings,
//  * - ints
//  * - longs
//  * - doubles
//  * - booleans
//  * - big decimals
//  * - seqs of Strings
//  * - seqs of case classes
//  * - lists of case classes
//  * - Maps of Strings to Ints
//  * - Maps of Strings to Case Classes
//  * - Sets of Strings
//  * - Options of Strings
//  * - Options of Case classes
//  * - Options of enumerations
//  * - Either[A,B] where A and B are both case classes
//  * - Either[A,B] where A and B are both primitives
//  */
//class UniverseTest extends WordSpec with Matchers {
//
//  val clipper = Ship(name = "Imperial Clipper", role = "fighter escort", maxSpeed = 430, jumpRange = 8.67, hardpoints = Map.empty, defaultWeapon = Some("pulse laser"))
//  val eagle = Ship(name = "Eagle", role = "fighter", maxSpeed = 350, jumpRange = 15.4, hardpoints = Map.empty, defaultWeapon = None)
//  val earth = Planet("Earth", "Sol", Some(PlanetClass.M))
//  val mars = Planet("Mars", "Sol", Some(PlanetClass.H))
//
//  val g = Universe(
//    manufacturers = List(
//      Manufacturer(
//        name = "Gutamaya",
//        ships = Seq(clipper)
//      ),
//      Manufacturer(
//        name = "Core Dynamics",
//        ships = Seq(eagle)
//      )
//    )
//  )
//
//  "Avro4s" should {
//    "support complex schema" in {
//      val schema = AvroSchema[Universe]
//      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/universe.json"))
//      schema.toString(true) shouldBe expected.toString(true)
//    }
//    "support complex write" in {
//      val output = new FileOutputStream("universe.avro")
//      val schema = AvroSchema[Universe]
//      val avro = AvroOutputStream.data[Universe].to(output).build(schema)
//      avro.write(g)
//      avro.close()
//    }
////    "support complex read" in {
////      val avro = AvroInputStream.data[Universe](Paths.get("universe.avro"), AvroSchema[Universe])
////      val universe = avro.iterator.next()
////      universe shouldBe g
////    }
//  }
//}
//
//case class Universe(manufacturers: List[Manufacturer])
//
//case class Faction(name: String, playable: Boolean, homeworld: Option[Planet], shipRanks: Map[String, Ship] = Map.empty, area: BigDecimal)
//
//object PlanetClass extends Enumeration {
//  val H, L, M = Value
//}
//
//case class Planet(name: String, system: String, planetClass: Option[PlanetClass.Value])
//
//case class Station(name: String)
//
//case class Manufacturer(name: String, ships: Seq[Ship])
//
//case class Ship(name: String, role: String, maxSpeed: Int, jumpRange: Double, hardpoints: Map[String, String], defaultWeapon: Option[String])
//
//case class CQC(maps: Seq[PlayableMap])
//
//case class PlayableMap(name: String, bonus: Either[String, Long], stationOrPlanet: Either[Station, Planet])