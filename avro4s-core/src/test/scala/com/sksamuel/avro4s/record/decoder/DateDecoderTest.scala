package com.sksamuel.avro4s.record.decoder

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import com.sksamuel.avro4s.SchemaForV2.TimestampNanosLogicalType
import com.sksamuel.avro4s.{AvroSchemaV2, Decoder, SchemaForV2}
import org.apache.avro.generic.GenericData
import org.apache.avro.{LogicalTypes, SchemaBuilder}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

//noinspection ScalaDeprecation
class DateDecoderTest extends AnyFunSuite with Matchers {

  case class WithLocalTime(z: LocalTime)
  case class WithLocalDate(z: LocalDate)
  case class WithDate(z: Date)
  case class WithLocalDateTime(z: LocalDateTime)
  case class WithTimestamp(z: Timestamp)
  case class WithInstant(z: Instant)

  test("decode int to LocalTime") {
    val schema = AvroSchemaV2[WithLocalTime]
    val record = new GenericData.Record(schema)
    record.put("z", 46245000000L)
    Decoder[WithLocalTime].decode(record) shouldBe WithLocalTime(LocalTime.of(12, 50, 45))
  }

  test("decode int to LocalDate") {
    val schema = AvroSchemaV2[WithLocalDate]
    val record = new GenericData.Record(schema)
    record.put("z", 17784)
    Decoder[WithLocalDate].decode(record) shouldBe WithLocalDate(LocalDate.of(2018, 9, 10))
  }

  test("decode int to java.sql.Date") {
    val schema = AvroSchemaV2[WithDate]
    val record = new GenericData.Record(schema)
    record.put("z", 17784)
    Decoder[WithDate].decode(record) shouldBe WithDate(Date.valueOf(LocalDate.of(2018, 9, 10)))
  }

  test("decode timestamp-millis to LocalDateTime") {
    val dateSchema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)
    val schema = SchemaBuilder.record("foo").fields().name("z").`type`(dateSchema).noDefault().endRecord()
    val record = new GenericData.Record(schema)
    record.put("z", 1572707106376L)
    Decoder[WithLocalDateTime].withSchema(SchemaForV2(schema)).decode(record) shouldBe WithLocalDateTime(
      LocalDateTime.of(2019, 11, 2, 15, 5, 6, 376000000))
  }

  test("decode timestamp-micros to LocalDateTime") {
    val dateSchema = LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder.longType)
    val schema = SchemaBuilder.record("foo").fields().name("z").`type`(dateSchema).noDefault().endRecord()
    val record = new GenericData.Record(schema)
    record.put("z", 1572707106376001L)
    Decoder[WithLocalDateTime].withSchema(SchemaForV2(schema)).decode(record) shouldBe WithLocalDateTime(
      LocalDateTime.of(2019, 11, 2, 15, 5, 6, 376001000))
  }

  test("decode timestamp-nanos to LocalDateTime") {
    val dateSchema = TimestampNanosLogicalType.addToSchema(SchemaBuilder.builder.longType)
    val schema = SchemaBuilder.record("foo").fields().name("z").`type`(dateSchema).noDefault().endRecord()
    val record = new GenericData.Record(schema)
    record.put("z", 1572707106376000002L)
    Decoder[WithLocalDateTime].decode(record) shouldBe WithLocalDateTime(
      LocalDateTime.of(2019, 11, 2, 15, 5, 6, 376000002))
  }

  test("decode long to Timestamp") {
    val schema = AvroSchemaV2[WithTimestamp]
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000L)
    Decoder[WithTimestamp].decode(record) shouldBe WithTimestamp(new Timestamp(1538312231000L))
  }

  test("decode long to Instant") {
    val schema = AvroSchemaV2[WithInstant]
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000L)
    Decoder[WithInstant].decode(record) shouldBe WithInstant(Instant.ofEpochMilli(1538312231000L))
  }
}
