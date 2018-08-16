package com.sksamuel.avro4s

import java.util.UUID

sealed trait Wibble
case class Wobble(str: String) extends Wibble
case class Wabble(dbl: Double) extends Wibble
case class Wrapper(wibble: Wibble)

sealed trait Tibble
case class Tobble(str: String, place: String) extends Tibble
case class Tabble(str: Double, age: Int) extends Tibble
case class Trapper(tibble: Tibble)

sealed trait Nibble
case class Nobble(str: String, place: String) extends Nibble
case class Nabble(str: String, age: Int) extends Nibble
case class Napper(nibble: Nibble)

case class Ids(myid: UUID)

case class Recursive(payload: Int, next: Option[Recursive])

case class MutRec1(payload: Int, children: List[MutRec2])
case class MutRec2(payload: String, children: List[MutRec1])

case class NestedSetDouble(set: Set[Double])
case class NestedSet(set: Set[Nested])
case class Nested(goo: String)
case class NestedBoolean(b: Boolean)

case class DefaultValues(
                          name: String = "sammy",
                          age: Int = 21,
                          isFemale: Boolean = false,
                          length: Double = 6.2,
                          timestamp: Long = 1468920998000l,
                          address: Map[String, String] = Map(
                            "home" -> "sammy's home address",
                            "work" -> "sammy's work address"
                          ),
                          traits: Seq[String] = Seq("Adventurous", "Helpful"),
                          favoriteWine: Wine = Wine.CabSav
                        )

