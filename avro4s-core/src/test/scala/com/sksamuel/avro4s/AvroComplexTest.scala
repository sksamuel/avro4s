package com.sksamuel.avro4s

import java.io.File

import org.scalatest.{Matchers, WordSpec}

// an end to end test for a hugely nested structure
class AvroComplexTest extends WordSpec with Matchers {

  "Avro4s" should {
    "support everything!" in {

      val g = Galaxy("milky way",
        Seq(
          Quadrant("alpha",
            Seq(
              Species("Human", "Earth", true, true),
              Species("Vulcan", "Vulcan", true, true)
            )
          ),
          Quadrant("beta",
            Seq(
              Species("Klingon", "Kronos", true, true)
            )
          )
        ),
        Seq(
          Org(
            "United Federation of Planets",
            Seq(
              Ship("enterprise", "galaxy", true, Some("picard")),
              Ship("enterprise", "luna", true, Some("riker")),
              Ship("enterprise", "galaxy", true, None)
            )
          )
        ),
        Map("earth" -> "hq", "wolf359" -> "borg battle")
      )

      import AvroImplicits._

      val out = AvroOutputStream[Galaxy](new File("galaxy.avro"))
      out.write(g)
      out.close()
    }
  }
}

case class Galaxy(name: String, quadrants: Seq[Quadrant], organizations: Seq[Org], famousStars: Map[String, String])

case class Org(name: String, ships: Seq[Ship])

case class Ship(name: String, `class`: String, flagship: Boolean, captain: Option[String])

case class Quadrant(name: String, species: Seq[Species])

case class Species(name: String, homeworld: String, extant: Boolean, peaceful: Boolean = true)

//Map(
//"enterprise" -> Ship("enterprise", "galaxy", true, Some("picard"), Map("max_warp" -> "9.9")),
//"titan" -> Ship("enterprise", "luna", true, Some("riker"), Map("named_for" -> "sol moons")),
//"enterprise" -> Ship("enterprise", "galaxy", true, None, Map.empty)
//)