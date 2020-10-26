package com.sksamuel.avro4s.record.encoder

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import com.sksamuel.avro4s.SchemaFor.TimestampNanosLogicalType
import com.sksamuel.avro4s.{AvroSchema, Encoder, ImmutableRecord, SchemaFor}
import org.apache.avro.{LogicalType, LogicalTypes, Schema, SchemaBuilder}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

//noinspection ScalaDeprecation
class DateEncoderTest extends AnyFunSuite with Matchers {

  test("encode LocalTime as TIME-MILLIS") {
    case class Foo(s: LocalTime)
    val schema = AvroSchema[Foo]
    Encoder[Foo].encode(Foo(LocalTime.of(12, 50, 45))) shouldBe expectedRecord(schema, 46245000000L)
  }

  test("encode LocalDate as DATE") {
    case class Foo(s: LocalDate)
    val schema = AvroSchema[Foo]
    Encoder[Foo].encode(Foo(LocalDate.of(2018, 9, 10))) shouldBe expectedRecord(schema, 17784)
  }

  test("encode java.sql.Date as DATE") {
    case class Foo(s: Date)
    val schema = AvroSchema[Foo]
    Encoder[Foo].encode(Foo(Date.valueOf(LocalDate.of(2018, 9, 10)))) shouldBe expectedRecord(schema, 17784)
  }

  test("encode LocalDateTime as timestamp-nanos") {
    case class Foo(s: LocalDateTime)
    val schema = AvroSchema[Foo]
    Encoder[Foo].encode(Foo(LocalDateTime.of(2018, 9, 10, 11, 58, 59, 123))) shouldBe expectedRecord(schema, 1536580739000000123L)
    Encoder[Foo].encode(Foo(LocalDateTime.of(2018, 9, 10, 11, 58, 59, 123009))) shouldBe expectedRecord(schema, 1536580739000123009L)
    Encoder[Foo].encode(Foo(LocalDateTime.of(2018, 9, 10, 11, 58, 59, 328187943))) shouldBe expectedRecord(schema, 1536580739328187943L)
  }

  test("encode LocalDateTime as timestamp-micros") {
    case class Foo(s: LocalDateTime)
    val schema = recordSchemaWithLogicalType(LogicalTypes.timestampMicros)
    Encoder[Foo].withSchema(SchemaFor(schema)).encode(Foo(LocalDateTime.of(2018, 9, 10, 11, 58, 59, 328187000))) shouldBe expectedRecord(schema, 1536580739328187L)
  }

  test("encode LocalDateTime as timestamp-millis") {
    case class Foo(s: LocalDateTime)
    val schema = recordSchemaWithLogicalType(LogicalTypes.timestampMillis)
    Encoder[Foo].withSchema(SchemaFor(schema)).encode(Foo(LocalDateTime.of(2018, 9, 10, 11, 58, 59, 328000000))) shouldBe expectedRecord(schema, 1536580739328L)
  }

    test("encode Timestamp as timestamp-millis") {
    case class Foo(s: Timestamp)
    val schema = AvroSchema[Foo]
    Encoder[Foo].encode(Foo(Timestamp.from(Instant.ofEpochMilli(1538312231000L)))) shouldBe expectedRecord(schema,1538312231000L)
  }

  test("encode Timestamp as timestamp-micros") {
    case class Foo(s: Timestamp)
    val schema = recordSchemaWithLogicalType(LogicalTypes.timestampMicros)
    Encoder[Foo].withSchema(SchemaFor(schema)).encode(Foo(Timestamp.from(Instant.ofEpochMilli(1538312231000L).plusNanos(1000)))) shouldBe expectedRecord(schema,1538312231000001L)
  }

  test("encode Timestamp as timestamp-nanos") {
    case class Foo(s: Timestamp)
    val schema = recordSchemaWithLogicalType(TimestampNanosLogicalType)
    Encoder[Foo].withSchema(SchemaFor(schema)).encode(Foo(Timestamp.from(Instant.ofEpochMilli(1538312231000L).plusNanos(1)))) shouldBe expectedRecord(schema,1538312231000000001L)
  }

  test("encode Instant as timestamp-millis") {
    case class Foo(s: Instant)
    val schema = AvroSchema[Foo]
    Encoder[Foo].encode(Foo(Instant.ofEpochMilli(1538312231000L))) shouldBe expectedRecord(schema, 1538312231000L)
  }

  test("encode Instant as timestamp-micros") {
    case class Foo(s: Instant)
    val schema = recordSchemaWithLogicalType(LogicalTypes.timestampMicros)
    Encoder[Foo].withSchema(SchemaFor(schema)).encode(Foo(Instant.ofEpochMilli(1538312231000L).plusNanos(1000))) shouldBe expectedRecord(schema, 1538312231000001L)
  }

  test("encode Instant as timestamp-nanos") {
    case class Foo(s: Instant)
    val schema = recordSchemaWithLogicalType(TimestampNanosLogicalType)
    Encoder[Foo].withSchema(SchemaFor(schema)).encode(Foo(Instant.ofEpochMilli(1538312231000L).plusNanos(1))) shouldBe expectedRecord(schema, 1538312231000000001L)
  }

  def recordSchemaWithLogicalType(logicalType: LogicalType): Schema = {
    val dateSchema = logicalType.addToSchema(SchemaBuilder.builder.longType)
    SchemaBuilder.record("foo").fields().name("s").`type`(dateSchema).noDefault().endRecord()
  }

  def expectedRecord(schema: Schema, value: Long): ImmutableRecord =
    ImmutableRecord(schema, Vector(java.lang.Long.valueOf(value)))

  def expectedRecord(schema: Schema, value: Int): ImmutableRecord =
    ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(value)))
}


