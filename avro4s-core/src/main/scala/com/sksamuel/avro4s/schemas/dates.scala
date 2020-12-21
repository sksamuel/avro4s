package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.SchemaFor
import org.apache.avro.{LogicalTypes, SchemaBuilder}

import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime}
import java.util.Date

trait DateSchemas:
  given InstantSchemaFor : SchemaFor[Instant] = SchemaFor[Instant](LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType))
  given DateSchemaFor : SchemaFor[Date] = SchemaFor(LogicalTypes.date().addToSchema(SchemaBuilder.builder.intType))
  given LocalDateSchemaFor : SchemaFor[LocalDate] = DateSchemaFor.forType
  given LocalDateTimeSchemaFor : SchemaFor[LocalDateTime] = SchemaFor(TimestampNanosLogicalType.addToSchema(SchemaBuilder.builder.longType))
  given OffsetDateTimeSchemaFor : SchemaFor[OffsetDateTime] = SchemaFor(OffsetDateTimeLogicalType.addToSchema(SchemaBuilder.builder.stringType))
  given LocalTimeSchemaFor : SchemaFor[Nothing] = SchemaFor(LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder.longType))
  given TimestampSchemaFor : SchemaFor[Timestamp] = SchemaFor[Timestamp](LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType))
