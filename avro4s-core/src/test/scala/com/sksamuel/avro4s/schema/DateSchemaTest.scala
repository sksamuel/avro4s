package com.sksamuel.avro4s.schema

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.{FunSuite, Matchers}

class DateSchemaTest extends FunSuite with Matchers {

  test("generate date logical type for LocalDate") {
    case class LocalDateTest(date: LocalDate)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/localdate.json"))
    val schema = AvroSchema[LocalDateTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate date logical type for Date") {
    case class DateTest(date: Date)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/date.json"))
    val schema = AvroSchema[DateTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate time logical type for LocalTime") {
    case class LocalTimeTest(time: LocalTime)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/localtime.json"))
    val schema = AvroSchema[LocalTimeTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate timestamp-nanos for LocalDateTime") {
    case class LocalDateTimeTest(time: LocalDateTime)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/localdatetime.json"))
    val schema = AvroSchema[LocalDateTimeTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate timestamp-millis logical type for Instant") {
    case class InstantTest(instant: Instant)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/instant.json"))
    val schema = AvroSchema[InstantTest]
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate timestamp-millis logical type for Timestamp") {
    case class TimestampTest(ts: Timestamp)
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/timestamp.json"))
    val schema = AvroSchema[TimestampTest]
    schema.toString(true) shouldBe expected.toString(true)
  }
}

