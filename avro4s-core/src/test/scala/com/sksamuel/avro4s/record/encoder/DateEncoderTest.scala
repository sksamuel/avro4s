package com.sksamuel.avro4s.record.encoder

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import com.sksamuel.avro4s.{AvroSchemaV2, DefaultFieldMapper, EncoderV2, ImmutableRecord}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

//noinspection ScalaDeprecation
class DateEncoderTest extends AnyFunSuite with Matchers {

  test("encode LocalTime as TIME-MILLIS") {
    case class Foo(s: LocalTime)
    val schema = AvroSchemaV2[Foo]
    EncoderV2[Foo].encode(Foo(LocalTime.of(12, 50, 45))) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(46245000000L)))
  }

  test("encode LocalDate as DATE") {
    case class Foo(s: LocalDate)
    val schema = AvroSchemaV2[Foo]
    EncoderV2[Foo].encode(Foo(LocalDate.of(2018, 9, 10))) shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(17784)))
  }

  test("encode java.sql.Date as DATE") {
    case class Foo(s: Date)
    val schema = AvroSchemaV2[Foo]
    EncoderV2[Foo].encode(Foo(Date.valueOf(LocalDate.of(2018, 9, 10)))) shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(17784)))
  }

  test("encode LocalDateTime as timestamp-nanos") {
    case class Foo(s: LocalDateTime)
    val schema = AvroSchemaV2[Foo]
    EncoderV2[Foo].encode(Foo(LocalDateTime.of(2018, 9, 10, 11, 58, 59, 123))) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(1536580739000000123L)))
    EncoderV2[Foo].encode(Foo(LocalDateTime.of(2018, 9, 10, 11, 58, 59, 123009))) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(1536580739000123009L)))
    EncoderV2[Foo].encode(Foo(LocalDateTime.of(2018, 9, 10, 11, 58, 59, 328187943))) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(1536580739328187943L)))
  }

  test("encode Timestamp as TIMESTAMP-MILLIS") {
    case class Foo(s: Timestamp)
    val schema = AvroSchemaV2[Foo]
    EncoderV2[Foo].encode(Foo(Timestamp.from(Instant.ofEpochMilli(1538312231000L)))) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(1538312231000L)))
  }

  test("encode Instant as TIMESTAMP-MILLIS") {
    case class Foo(s: Instant)
    val schema = AvroSchemaV2[Foo]
    EncoderV2[Foo].encode(Foo(Instant.ofEpochMilli(1538312231000L))) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(1538312231000L)))
  }
}


