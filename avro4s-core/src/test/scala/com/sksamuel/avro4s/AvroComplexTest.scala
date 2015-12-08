package com.sksamuel.avro4s

import org.scalatest.{Matchers, WordSpec}

// an end to end test for a hugely nested structure
class AvroComplexTest extends WordSpec with Matchers {

  val g = Galaxy(
    Seq(
      Org(
        Map("picard" -> "captain of enterprise; former borg")
      )
    )
  )

  "Avro4s" should {
    "support complex schema" in {
     // val schema = AvroImplicits.schemaFor[Galaxy]
      //println(schema)
    }
    "support complex write" in {
      //      val out = AvroOutputStream[Galaxy](new File("galaxy.avro"))
      //      out.write(g)
      //      out.close()
    }
  }
}

case class Galaxy(organizations: Seq[Org])

case class Org( famousPeople: Map[String, String])

case class Ship(name: String, `class`: String, flagship: Boolean, captain: Option[String])

case class Quadrant(name: String, species: Seq[Species])

case class Species(name: String, homeworld: String, extant: Boolean, peaceful: Boolean = true)

//Map(
//"enterprise" -> Ship("enterprise", "galaxy", true, Some("picard"), Map("max_warp" -> "9.9")),
//"titan" -> Ship("enterprise", "luna", true, Some("riker"), Map("named_for" -> "sol moons")),
//"enterprise" -> Ship("enterprise", "galaxy", true, None, Map.empty)
//)