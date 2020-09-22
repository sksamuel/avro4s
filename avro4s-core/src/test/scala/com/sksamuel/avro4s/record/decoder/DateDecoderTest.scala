package com.sksamuel.avro4s.record.decoder

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import com.sksamuel.avro4s.SchemaFor.TimestampNanosLogicalType
import com.sksamuel.avro4s.{AvroSchema, Decoder, SchemaFor}
import org.apache.avro.generic.GenericData
import org.apache.avro.{LogicalType, LogicalTypes, Schema, SchemaBuilder}
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
    val schema = AvroSchema[WithLocalTime]
    val record = new GenericData.Record(schema)
    record.put("z", 46245000000L)
    Decoder[WithLocalTime].decode(record) shouldBe WithLocalTime(LocalTime.of(12, 50, 45))
  }

  test("decode int to LocalDate") {
    val schema = AvroSchema[WithLocalDate]
    val record = new GenericData.Record(schema)
    record.put("z", 17784)
    Decoder[WithLocalDate].decode(record) shouldBe WithLocalDate(LocalDate.of(2018, 9, 10))
  }

  test("decode int to java.sql.Date") {
    val schema = AvroSchema[WithDate]
    val record = new GenericData.Record(schema)
    record.put("z", 17784)
    Decoder[WithDate].decode(record) shouldBe WithDate(Date.valueOf(LocalDate.of(2018, 9, 10)))
  }

  test("decode timestamp-millis to LocalDateTime") {
    val schema = recordSchemaWithLogicalType(LogicalTypes.timestampMillis)
    val record = new GenericData.Record(schema)
    record.put("z", 1572707106376L)
    Decoder[WithLocalDateTime].withSchema(SchemaFor(schema)).decode(record) shouldBe WithLocalDateTime(
      LocalDateTime.of(2019, 11, 2, 15, 5, 6, 376000000))
  }

  test("decode timestamp-micros to LocalDateTime") {
    val schema = recordSchemaWithLogicalType(LogicalTypes.timestampMicros)
    val record = new GenericData.Record(schema)
    record.put("z", 1572707106376001L)
    Decoder[WithLocalDateTime].withSchema(SchemaFor(schema)).decode(record) shouldBe WithLocalDateTime(
      LocalDateTime.of(2019, 11, 2, 15, 5, 6, 376001000))
  }

  test("decode timestamp-nanos to LocalDateTime") {
    val schema = recordSchemaWithLogicalType(TimestampNanosLogicalType)
    val record = new GenericData.Record(schema)
    record.put("z", 1572707106376000002L)
    Decoder[WithLocalDateTime].decode(record) shouldBe WithLocalDateTime(
      LocalDateTime.of(2019, 11, 2, 15, 5, 6, 376000002))
  }

  test("decode long to Timestamp") {
    val schema = AvroSchema[WithTimestamp]
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000L)
    Decoder[WithTimestamp].decode(record) shouldBe WithTimestamp(new Timestamp(1538312231000L))
  }

  test("decode timestamp-millis to Timestamp") {
    val schema = recordSchemaWithLogicalType(LogicalTypes.timestampMillis)
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000L)
    Decoder[WithTimestamp].withSchema(SchemaFor(schema)).decode(record) shouldBe WithTimestamp(new Timestamp(1538312231000L))
  }

  test("decode timestamp-micros to Timestamp") {
    val schema = recordSchemaWithLogicalType(LogicalTypes.timestampMicros)
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000001L)
    val t = new Timestamp(1538312231000L)
    t.setNanos(1000)
    Decoder[WithTimestamp].withSchema(SchemaFor(schema)).decode(record) shouldBe WithTimestamp(timestamp(1538312231000L,1000))
  }

  test("decode timestamp-nanos to Timestamp") {
    val schema = recordSchemaWithLogicalType(TimestampNanosLogicalType)
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000000001L)
    Decoder[WithTimestamp].withSchema(SchemaFor(schema)).decode(record) shouldBe WithTimestamp(timestamp(1538312231000L, 1))
  }

  test("decode long to Instant") {
    val schema = AvroSchema[WithInstant]
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000L)
    Decoder[WithInstant].decode(record) shouldBe WithInstant(Instant.ofEpochMilli(1538312231000L))
  }

  test("decode timestamp-millis to Instant") {
    val schema = recordSchemaWithLogicalType(LogicalTypes.timestampMillis)
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000L)
    Decoder[WithInstant].decode(record) shouldBe WithInstant(Instant.ofEpochMilli(1538312231000L))
  }

  test("decode timestamp-micros to Instant") {
    val schema = recordSchemaWithLogicalType(LogicalTypes.timestampMicros)
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000001L)
    Decoder[WithInstant].withSchema(SchemaFor(schema)).decode(record) shouldBe WithInstant(Instant.ofEpochMilli(1538312231000L).plusNanos(1000))
  }

  test("decode timestamp-nanos to Instant") {
    val schema = recordSchemaWithLogicalType(TimestampNanosLogicalType)
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000000001L)
    Decoder[WithInstant].withSchema(SchemaFor(schema)).decode(record) shouldBe WithInstant(Instant.ofEpochMilli(1538312231000L).plusNanos(1))
  }

  def timestamp(millis: Long, nanos: Int = 0): Timestamp = {
    val t = new Timestamp(millis)
    t.setNanos(nanos)
    t
  }

  def recordSchemaWithLogicalType(logicalType: LogicalType): Schema = {
    val dateSchema = logicalType.addToSchema(SchemaBuilder.builder.longType)
    SchemaBuilder.record("foo").fields().name("z").`type`(dateSchema).noDefault().endRecord()
  }
}
