package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema, AvroSortPriority}

import org.scalatest.{FunSuite, Matchers}

class AvroSortPrioritySchemaTest extends FunSuite with Matchers {

  test("enums should be sorted by descending priority") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_sort_priority_enum.json"))
    val schema = AvroSchema[Numeric]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("unions should be sorted by descending priority") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_sort_priority_union.json"))
    val schema = AvroSchema[FightingStyleWrapper]

    schema.toString(true) shouldBe expected.toString(true)
  }
}


sealed trait Numeric
@AvroSortPriority(1)
case object RationalNumber extends Numeric
@AvroSortPriority(0)
case object RealNumber extends Numeric
@AvroSortPriority(2)
case object NaturalNumber extends Numeric


case class FightingStyleWrapper(fightingstyle: FightingStyle)

sealed trait FightingStyle
@AvroSortPriority(2)
case class AggressiveFightingStyle(agressiveness: Float) extends FightingStyle
@AvroSortPriority(10)
case class DefensiveFightingStlye(has_armor: Boolean) extends FightingStyle
