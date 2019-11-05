package com.sksamuel.avro4s.record.encoder

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import com.sksamuel.avro4s.{AvroSchema, DefaultFieldMapper, Encoder, ImmutableRecord}
import org.scalatest.{FunSuite, Matchers}

//noinspection ScalaDeprecation
class DateEncoderTest extends FunSuite with Matchers {

  test("encode LocalTime as TIME-MILLIS") {
    case class Foo(s: LocalTime)
    val schema = AvroSchema[Foo]
    Encoder[Foo].encode(Foo(LocalTime.of(12, 50, 45)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(46245000000L)))
  }

  test("encode LocalDate as DATE") {
    case class Foo(s: LocalDate)
    val schema = AvroSchema[Foo]
    Encoder[Foo].encode(Foo(LocalDate.of(2018, 9, 10)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(17784)))
  }

  test("encode java.sql.Date as DATE") {
    case class Foo(s: Date)
    val schema = AvroSchema[Foo]
    Encoder[Foo].encode(Foo(Date.valueOf(LocalDate.of(2018, 9, 10))), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(17784)))
  }

  test("encode LocalDateTime as timestamp-nanos") {
    case class Foo(s: LocalDateTime)
    val schema = AvroSchema[Foo]
    Encoder[Foo].encode(Foo(LocalDateTime.of(2018, 9, 10, 11, 58, 59, 123)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(1536580739000000123L)))
    Encoder[Foo].encode(Foo(LocalDateTime.of(2018, 9, 10, 11, 58, 59, 123009)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(1536580739000123009L)))
    Encoder[Foo].encode(Foo(LocalDateTime.of(2018, 9, 10, 11, 58, 59, 328187943)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(1536580739328187943L)))
  }

  test("encode Timestamp as TIMESTAMP-MILLIS") {
    case class Foo(s: Timestamp)
    val schema = AvroSchema[Foo]
    Encoder[Foo].encode(Foo(Timestamp.from(Instant.ofEpochMilli(1538312231000L))), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(1538312231000L)))
  }

  test("encode Instant as TIMESTAMP-MILLIS") {
    case class Foo(s: Instant)
    val schema = AvroSchema[Foo]
    Encoder[Foo].encode(Foo(Instant.ofEpochMilli(1538312231000L)), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(java.lang.Long.valueOf(1538312231000L)))
  }
}


