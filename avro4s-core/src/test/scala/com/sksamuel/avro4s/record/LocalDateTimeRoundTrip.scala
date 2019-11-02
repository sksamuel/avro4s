package com.sksamuel.avro4s.record

import java.time.LocalDateTime

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultFieldMapper, Encoder, FieldMapper, SchemaFor}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.scalatest.{FunSuite, Matchers}

class LocalDateTimeRoundTrip extends FunSuite with Matchers {

  test("local date time round trip") {

    val localDateTime = LocalDateTime.of(2018, 1, 1, 23, 30, 5, 328187943)

    val encodedLocalDateTime = Encoder[LocalDateTime].encode(
      localDateTime,
      AvroSchema[LocalDateTime],
      DefaultFieldMapper
    )

    Decoder[LocalDateTime]
      .decode(encodedLocalDateTime, AvroSchema[LocalDateTime], DefaultFieldMapper) shouldEqual localDateTime
  }

  test("local date time round trip with timestamp-micros") {

    val localDateTime = LocalDateTime.of(2018, 1, 1, 23, 30, 5, 328187000)

    val schema = LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder().longType())

    val encodedLocalDateTime = Encoder[LocalDateTime].encode(
      localDateTime,
      schema,
      DefaultFieldMapper
    )

    Decoder[LocalDateTime]
      .decode(encodedLocalDateTime, schema, DefaultFieldMapper) shouldEqual localDateTime
  }
}
