package com.sksamuel.avro4s

import java.sql.{Date, Timestamp}
import java.time._
import java.time.format.DateTimeFormatter

import com.sksamuel.avro4s.SchemaFor.TimestampNanosLogicalType
import org.apache.avro.LogicalTypes.{TimeMicros, TimeMillis, TimestampMicros, TimestampMillis}

trait TemporalEncoders {
  implicit val InstantEncoder =
    Encoder.LongEncoder.comap[Instant](_.toEpochMilli).withSchema(SchemaFor.InstantSchemaFor)

  implicit val LocalTimeEncoder: Encoder[LocalTime] = new Encoder[LocalTime] {
    val schemaFor: SchemaFor[LocalTime] = SchemaFor.LocalTimeSchemaFor

    def encode(value: LocalTime): AnyRef = java.lang.Long.valueOf(value.toNanoOfDay / 1000)
  }

  implicit val LocalDateEncoder: Encoder[LocalDate] =
    Encoder.IntEncoder.comap[LocalDate](_.toEpochDay.toInt).withSchema(SchemaFor.LocalDateSchemaFor)

  implicit val TimestampEncoder: Encoder[Timestamp] = InstantEncoder.comap[Timestamp](_.toInstant)

  implicit val DateEncoder: Encoder[Date] = LocalDateEncoder.comap[Date](_.toLocalDate)

  implicit val LocalDateTimeEncoder: Encoder[LocalDateTime] = new LocalDateTimeEncoder(SchemaFor.LocalDateTimeSchemaFor)

  private class LocalDateTimeEncoder(val schemaFor: SchemaFor[LocalDateTime]) extends Encoder[LocalDateTime] {

    val encoder: LocalDateTime => Long = schemaFor.schema.getLogicalType match {
      case _: TimestampMillis => _.toInstant(ZoneOffset.UTC).toEpochMilli
      case _: TimestampMicros =>
        t =>
          t.toEpochSecond(ZoneOffset.UTC) * 1000000L + t.getNano.toLong / 1000L
      case TimestampNanosLogicalType =>
        t =>
          t.toEpochSecond(ZoneOffset.UTC) * 1000000000L + t.getNano.toLong
      case _ => throw new Avro4sConfigurationException(s"Unsupported type for LocalDateTime: ${schemaFor.schema}")
    }

    def encode(t: LocalDateTime): AnyRef = java.lang.Long.valueOf(encoder(t))

    override def withSchema(schemaFor: SchemaFor[LocalDateTime]): Encoder[LocalDateTime] =
      new LocalDateTimeEncoder(schemaFor)
  }

  implicit object OffsetDateTimeEncoder extends Encoder[OffsetDateTime] {
    val schemaFor: SchemaFor[OffsetDateTime] = SchemaFor.OffsetDateTimeSchemaFor
    override def encode(value: OffsetDateTime) = value.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }
}

trait TemporalDecoders {
  implicit val InstantDecoder =
    Decoder.LongDecoder.map[Instant](Instant.ofEpochMilli).withSchema(SchemaFor.InstantSchemaFor)

  implicit val LocalTimeDecoder: Decoder[LocalTime] = new Decoder[LocalTime] {
    val schemaFor: SchemaFor[LocalTime] = SchemaFor.LocalTimeSchemaFor

    def decode(value: Any): LocalTime = schema.getLogicalType match {
      case _: TimeMillis =>
        value match {
          case i: Int  => LocalTime.ofNanoOfDay(i.toLong * 1000000L)
          case l: Long => LocalTime.ofNanoOfDay(l * 1000000L)
        }
      case _: TimeMicros =>
        value match {
          case i: Int  => LocalTime.ofNanoOfDay(i.toLong * 1000L)
          case l: Long => LocalTime.ofNanoOfDay(l * 1000L)
        }
    }
  }

  implicit val LocalDateDecoder: Decoder[LocalDate] =
    Decoder.IntDecoder.map[LocalDate](i => LocalDate.ofEpochDay(i.toLong)).withSchema(SchemaFor.LocalDateSchemaFor)

  implicit val TimestampDecoder: Decoder[Timestamp] = InstantDecoder.map[Timestamp](Timestamp.from)

  implicit val DateDecoder: Decoder[Date] = LocalDateDecoder.map[Date](Date.valueOf)

  implicit val LocalDateTimeDecoder: Decoder[LocalDateTime] = new LocalDateTimeDecoder(SchemaFor.LocalDateTimeSchemaFor)

  class LocalDateTimeDecoder(val schemaFor: SchemaFor[LocalDateTime]) extends Decoder[LocalDateTime] {

    val decoder: Any => LocalDateTime = schema.getLogicalType match {
      case _: TimestampMillis => {
        case i: Int  => LocalDateTime.ofInstant(Instant.ofEpochMilli(i.toLong), ZoneOffset.UTC)
        case l: Long => LocalDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneOffset.UTC)
      }

      case _: TimestampMicros => {
        case i: Int =>
          LocalDateTime.ofInstant(Instant.ofEpochMilli(i / 1000), ZoneOffset.UTC).plusNanos(i % 1000 * 1000)
        case l: Long =>
          LocalDateTime.ofInstant(Instant.ofEpochMilli(l / 1000), ZoneOffset.UTC).plusNanos(l % 1000 * 1000)
      }

      case TimestampNanosLogicalType => {
        case l: Long =>
          val nanos = l % 1000000
          LocalDateTime.ofInstant(Instant.ofEpochMilli(l / 1000000), ZoneOffset.UTC).plusNanos(nanos)
        case other => throw new Avro4sConfigurationException(s"Unsupported type for timestamp nanos ${other.getClass.getName}")
      }
    }

    def decode(value: Any): LocalDateTime = decoder(value)

    override def withSchema(schemaFor: SchemaFor[LocalDateTime]): Decoder[LocalDateTime] =
      new LocalDateTimeDecoder(schemaFor)
  }

  implicit object OffsetDateTimeDecoder extends Decoder[OffsetDateTime] {

    val schemaFor: SchemaFor[OffsetDateTime] = SchemaFor.OffsetDateTimeSchemaFor

    def decode(value: Any): OffsetDateTime =
      OffsetDateTime.parse(value.toString, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }
}