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
//  * - Either[A,B] where A and B are both case classes
//  * - Either[A,B] where A and B are both primitives
//  */
//class UniverseTest extends WordSpec with Matchers {
//
//  val clipper = Ship(name = "Imperial Clipper")
//  val eagle = Ship(name = "Eagle")
//
//  val g = Universe(
//    factions = Seq(
//      Faction(ships = Map("baron" -> clipper))
//    )
//  )
//
//  "Avro4s" should {
//    "support complex schema" in {
//      val schema = SchemaFor[Universe].apply()
//      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/universe.avsc"))
//      println(schema.toString(true))
//      schema.toString(true) shouldBe expected.toString(true)
//    }
//    "support complex write" in {
//      val output = new FileOutputStream("universe.avro")
//      val avro = AvroOutputStream[Universe](output)
//      avro.write(g)
//      avro.close()
//    }
//    "support complex read" in {
////      val avro = AvroInputStream[Universe](Paths.get("universe.avro"))
////      val universe = avro.iterator.next()
////      universe shouldBe g
//    }
//  }
//}
//
//case class Universe(factions: Seq[Faction])
//case class Faction(ships: Map[String, Ship])
//case class Ship(name: String)