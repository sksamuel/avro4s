package com.sksamuel.avro4s.record

import java.time.LocalDateTime

import com.sksamuel.avro4s.{Decoder, EncoderV2}
import org.apache.avro.{LogicalTypes, SchemaBuilder}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LocalDateTimeRoundTrip extends AnyFunSuite with Matchers {

  test("local date time round trip") {

    val localDateTime = LocalDateTime.of(2018, 1, 1, 23, 30, 5, 328187943)

    val encodedLocalDateTime = EncoderV2[LocalDateTime].encode(localDateTime)

    Decoder[LocalDateTime].decode(encodedLocalDateTime) shouldEqual localDateTime
  }

  test("local date time round trip with timestamp-micros") {

    val localDateTime = LocalDateTime.of(2018, 1, 1, 23, 30, 5, 328187000)

    val schema = LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder().longType())

    val encodedLocalDateTime = EncoderV2[LocalDateTime].encode(localDateTime)

    Decoder[LocalDateTime].decode(encodedLocalDateTime) shouldEqual localDateTime
  }
}
