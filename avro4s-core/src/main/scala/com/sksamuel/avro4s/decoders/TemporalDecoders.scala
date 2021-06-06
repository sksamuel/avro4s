package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.schemas.TimestampNanosLogicalType
import com.sksamuel.avro4s.{Avro4sConfigurationException, Decoder, SchemaFor}
import org.apache.avro.LogicalTypes.{TimeMicros, TimeMillis, TimestampMicros, TimestampMillis}
import org.apache.avro.Schema

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, ZoneOffset}

trait TemporalDecoders:

  given InstantDecoder: Decoder[Instant] = new TemporalWithLogicalTypeDecoder[Instant] {
    def ofEpochMillis(millis: Long): Instant = Instant.ofEpochMilli(millis)
    def ofEpochSeconds(seconds: Long, nanos: Long): Instant = Instant.ofEpochSecond(seconds, nanos)
  }

  given TimestampDecoder: Decoder[Timestamp] = InstantDecoder.map[Timestamp](Timestamp.from)

  given DateDecoder: Decoder[Date] = LocalDateDecoder.map[Date](Date.valueOf)

  given OffsetDateTimeDecoder: Decoder[OffsetDateTime] = new Decoder[OffsetDateTime] {
    override def decode(schema: Schema): Any => OffsetDateTime = { value =>
      OffsetDateTime.parse(value.toString, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    }
  }

  given LocalDateTimeDecoder: Decoder[LocalDateTime] = new TemporalWithLogicalTypeDecoder[LocalDateTime] {

    def ofEpochMillis(millis: Long): LocalDateTime =
      LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC)

    def ofEpochSeconds(seconds: Long, nanos: Long): LocalDateTime =
      LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), ZoneOffset.UTC)
  }

  given LocalTimeDecoder: Decoder[LocalTime] = new Decoder[LocalTime] {

    override def decode(schema: Schema): Any => LocalTime = {

      val toNanosFactor = schema.getLogicalType match {
        case _: TimeMicros => 1000L
        case _: TimeMillis => 1000000L
        case _ => throw new Avro4sConfigurationException(s"Unsupported logical type for LocalTime: ${schema}")
      }

      { value =>
        value match {
          case l: Long => LocalTime.ofNanoOfDay(l * toNanosFactor)
          case i: Int => LocalTime.ofNanoOfDay(i.toLong * toNanosFactor)
        }
      }
    }
  }

  given LocalDateDecoder: Decoder[LocalDate] =
    Decoder.IntDecoder.map[LocalDate](i => LocalDate.ofEpochDay(i.toLong))


private abstract class TemporalWithLogicalTypeDecoder[T] extends Decoder[T] {

  def ofEpochMillis(millis: Long): T
  def ofEpochSeconds(seconds: Long, nanos: Long): T

  override def decode(schema: Schema): Any => T = {

    val decoder: Any => T = schema.getLogicalType match {
      case _: TimestampMillis => {
        case l: Long => ofEpochMillis(l)
        case i: Int => ofEpochMillis(i.toLong)
      }

      case _: TimestampMicros => {
        case l: Long => ofEpochSeconds(l / 1000000, l % 1000000 * 1000)
        case i: Int => ofEpochSeconds(i / 1000000, i % 1000000 * 1000)
      }

      case TimestampNanosLogicalType => {
        case l: Long => ofEpochSeconds(l / 1000000000, l % 1000000000)
        case other =>
          throw new Avro4sConfigurationException(s"Unsupported type for timestamp nanos ${other.getClass.getName}")
      }

      case _ => throw new Avro4sConfigurationException(s"Unsupported logical type for temporal: ${schema}")
    }

    { value => decoder(value) }
  }
}