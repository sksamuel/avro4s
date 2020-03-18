package com.sksamuel.avro4s.schema

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import com.sksamuel.avro4s.AvroSchemaV2
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DateSchemaTest extends AnyFunSuite with Matchers {

  test("generate date logical type for LocalDate") {
    case class LocalDateTest(date: LocalDate)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/localdate.json"))
    val schema = AvroSchemaV2[LocalDateTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate date logical type for Date") {
    case class DateTest(date: Date)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/date.json"))
    val schema = AvroSchemaV2[DateTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate time logical type for LocalTime") {
    case class LocalTimeTest(time: LocalTime)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/localtime.json"))
    val schema = AvroSchemaV2[LocalTimeTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate timestamp-nanos for LocalDateTime") {
    case class LocalDateTimeTest(time: LocalDateTime)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/localdatetime.json"))
    val schema = AvroSchemaV2[LocalDateTimeTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate timestamp-millis logical type for Instant") {
    case class InstantTest(instant: Instant)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/instant.json"))
    val schema = AvroSchemaV2[InstantTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate timestamp-millis logical type for Timestamp") {
    case class TimestampTest(ts: Timestamp)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/timestamp.json"))
    val schema = AvroSchemaV2[TimestampTest]
    schema.toString(true) shouldBe expected.toString(true)
  }
}

