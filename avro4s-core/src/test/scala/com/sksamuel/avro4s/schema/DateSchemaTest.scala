package com.sksamuel.avro4s.schema

import java.time.{Instant, LocalDate, LocalTime}

import com.sksamuel.avro4s.internal.SchemaEncoder
import org.scalatest.{FunSuite, Matchers}

class DateSchemaTest extends FunSuite with Matchers {

  test("generate date logical type for LocalDate") {
    case class LocalDateTest(date: LocalDate)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/localdate.json"))
    val schema = SchemaEncoder[LocalDateTest].encode()
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate time logical type for LocalTime") {
    case class LocalTimeTest(time: LocalTime)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/localtime.json"))
    val schema = SchemaEncoder[LocalTimeTest].encode()
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate timestamp-millis logical type for Instant") {
    case class InstantTest(instant: Instant)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/timestamp_millis.json"))
    val schema = SchemaEncoder[InstantTest].encode()
    schema.toString(true) shouldBe expected.toString(true)
  }
}

