package com.sksamuel.avro4s.record.decoder

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import com.sksamuel.avro4s.AvroSchema
import com.sksamuel.avro4s.Decoder
import org.apache.avro.generic.GenericData
import org.scalatest.{FunSuite, Matchers}

//noinspection ScalaDeprecation
class DateDecoderTest extends FunSuite with Matchers {

  case class WithLocalTime(z: LocalTime)
  case class WithLocalDate(z: LocalDate)
  case class WithDate(z: Date)
  case class WithLocalDateTime(z: LocalDateTime)
  case class WithTimestamp(z: Timestamp)
  case class WithInstant(z: Instant)

  test("decode int to LocalTime") {
    val schema = AvroSchema[WithLocalTime]
    val record = new GenericData.Record(schema)
    record.put("z", 46245000)
    Decoder[WithLocalTime].decode(record, schema) shouldBe WithLocalTime(LocalTime.of(12, 50, 45))
  }

  test("decode int to LocalDate") {
    val schema = AvroSchema[WithLocalDate]
    val record = new GenericData.Record(schema)
    record.put("z", 17784)
    Decoder[WithLocalDate].decode(record, schema) shouldBe WithLocalDate(LocalDate.of(2018, 9, 10))
  }

  test("decode int to java.sql.Date") {
    val schema = AvroSchema[WithDate]
    val record = new GenericData.Record(schema)
    record.put("z", 17784)
    Decoder[WithDate].decode(record, schema) shouldBe WithDate(Date.valueOf(LocalDate.of(2018, 9, 10)))
  }

  test("decode long to LocalDateTime") {
    val schema = AvroSchema[WithLocalDateTime]
    val record = new GenericData.Record(schema)
    record.put("z", 1536580739000L)
    Decoder[WithLocalDateTime].decode(record, schema) shouldBe WithLocalDateTime(LocalDateTime.of(2018, 9, 10, 11, 58, 59))
  }

  test("decode long to Timestamp") {
    val schema = AvroSchema[WithTimestamp]
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000L)
    Decoder[WithTimestamp].decode(record, schema) shouldBe WithTimestamp(new Timestamp(1538312231000L))
  }

  test("decode long to Instant") {
    val schema = AvroSchema[WithInstant]
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000L)
    Decoder[WithInstant].decode(record, schema) shouldBe WithInstant(Instant.ofEpochMilli(1538312231000L))
  }
}


