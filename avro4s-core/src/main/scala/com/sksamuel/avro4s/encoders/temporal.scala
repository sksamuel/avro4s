package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.schemas.TimestampNanosLogicalType
import com.sksamuel.avro4s.{Avro4sConfigurationException, Encoder, SchemaFor}
import org.apache.avro.LogicalTypes.{TimeMicros, TimeMillis, TimestampMicros, TimestampMillis}
import org.apache.avro.Schema

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, ZoneOffset}
import java.sql.Date

trait TemporalEncoders:
  given Encoder[Instant] = InstantEncoder
  given TimestampEncoder: Encoder[Timestamp] = InstantEncoder.contramap[Timestamp](_.toInstant)

  given LocalDateEncoder: Encoder[LocalDate] = IntEncoder.contramap[LocalDate](_.toEpochDay.toInt)
  given Encoder[LocalTime] = LocalTimeEncoder
  given Encoder[LocalDateTime] = LocalDateTimeEncoder

  given DateEncoder: Encoder[Date] = IntEncoder.contramap[Date](_.toLocalDate.toEpochDay.toInt)
  given Encoder[OffsetDateTime] = OffsetDateTimeEncoder

object OffsetDateTimeEncoder extends Encoder[OffsetDateTime] :
  override def encode(schema: Schema): OffsetDateTime => AnyRef = { value =>
    value.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }

object LocalTimeEncoder extends Encoder[LocalTime] :
  override def encode(schema: Schema): LocalTime => AnyRef = {
    val toNanosFactor = schema.getLogicalType match {
      case _: TimeMicros => 1000L
      case _: TimeMillis => 1000000L
      case _ => throw new Avro4sConfigurationException(s"Unsupported logical type for LocalTime: ${schema}")
    }
    { value => java.lang.Long.valueOf(value.toNanoOfDay / toNanosFactor) }
  }

object LocalDateTimeEncoder extends TemporalWithLogicalTypeEncoder[LocalDateTime] :
  def epochMillis(temporal: LocalDateTime): Long = temporal.toInstant(ZoneOffset.UTC).toEpochMilli
  def epochSeconds(temporal: LocalDateTime): Long = temporal.toEpochSecond(ZoneOffset.UTC)
  def nanos(temporal: LocalDateTime): Long = temporal.getNano.toLong

object InstantEncoder extends TemporalWithLogicalTypeEncoder[Instant] :
  def epochMillis(temporal: Instant): Long = temporal.toEpochMilli
  def epochSeconds(temporal: Instant): Long = temporal.getEpochSecond
  def nanos(temporal: Instant): Long = temporal.getNano.toLong

private abstract class TemporalWithLogicalTypeEncoder[T] extends Encoder[T] :

  def epochMillis(temporal: T): Long
  def epochSeconds(temporal: T): Long
  def nanos(temporal: T): Long

  override def encode(schema: Schema): T => AnyRef = {
    val toLong: T => Long = schema.getLogicalType match {
      case _: TimestampMillis => epochMillis
      case _: TimestampMicros => t => epochSeconds(t) * 1000000L + nanos(t) / 1000L
      case TimestampNanosLogicalType => t => epochSeconds(t) * 1000000000L + nanos(t)
      case _ => throw new Avro4sConfigurationException(s"Unsupported logical type for temporal: ${schema}")
    }
    { value => java.lang.Long.valueOf(toLong(value)) }
  }
