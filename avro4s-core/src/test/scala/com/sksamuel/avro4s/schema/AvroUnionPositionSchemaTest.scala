package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.{AvroSchema, AvroUnionPosition}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AvroUnionPositionSchemaTest extends AnyFunSuite with Matchers with AvroUnionPositionSchemaTestContext {

  test("enums should be sorted by ascending union position") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_union_position_enum.json"))
    val schema = AvroSchema[Numeric]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("unions should be sorted by ascending union position") {
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_union_position_union.json"))
    val schema = AvroSchema[FightingStyleWrapper]

    schema.toString(true) shouldBe expected.toString(true)
  }

//  test("avrosortpriority should respect union default ordering") {
//    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/avro_sort_priority_union_with_default.json"))
//    val schema = AvroSchema[FightingStyleWrapperWithDefault]
//
//    schema.toString(true) shouldBe expected.toString(true)
//  }
}

trait AvroUnionPositionSchemaTestContext {

  sealed trait Numeric
  @AvroUnionPosition(2)
  case object RationalNumber extends Numeric
  @AvroUnionPosition(3)
  case object RealNumber extends Numeric
  @AvroUnionPosition(1)
  case object NaturalNumber extends Numeric

  case class FightingStyleWrapper(fightingstyle: FightingStyle)
//  case class FightingStyleWrapperWithDefault(fightingstyle: FightingStyle = AggressiveFightingStyle(10))

  sealed trait FightingStyle
  @AvroUnionPosition(2)
  case class AggressiveFightingStyle(agressiveness: Float) extends FightingStyle
  @AvroUnionPosition(1)
  case class DefensiveFightingStyle(has_armor: Boolean) extends FightingStyle
}
