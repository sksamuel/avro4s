package com.sksamuel.avro4s

import java.sql.{Date, Timestamp}
import java.time._
import java.time.format.DateTimeFormatter

import com.sksamuel.avro4s.SchemaFor.TimestampNanosLogicalType
import com.sksamuel.avro4s.Temporals.{TemporalWithLogicalTypeDecoder, TemporalWithLogicalTypeEncoder}
import org.apache.avro.LogicalTypes.{TimeMicros, TimeMillis, TimestampMicros, TimestampMillis}

trait TemporalEncoders {
  implicit val InstantEncoder =
    Encoder.LongEncoder.comap[Instant](_.toEpochMilli).withSchema(SchemaFor.InstantSchemaFor)

  private final class InstantEncoder(val schemaFor: SchemaFor[Instant])
      extends TemporalWithLogicalTypeEncoder[Instant] {
    def epochMillis(temporal: Instant): Long = temporal.toEpochMilli

    def epochSeconds(temporal: Instant): Long = temporal.getEpochSecond

    def nanos(temporal: Instant): Long = temporal.getNano.toLong

    override def withSchema(schemaFor: SchemaFor[Instant]): Encoder[Instant] =
      new InstantEncoder(schemaFor)
  }

  implicit val LocalTimeEncoder: Encoder[LocalTime] = new Encoder[LocalTime] {
    val schemaFor: SchemaFor[LocalTime] = SchemaFor.LocalTimeSchemaFor

    def encode(value: LocalTime): AnyRef = java.lang.Long.valueOf(value.toNanoOfDay / 1000)
  }

  implicit val LocalDateEncoder: Encoder[LocalDate] =
    Encoder.IntEncoder.comap[LocalDate](_.toEpochDay.toInt).withSchema(SchemaFor.LocalDateSchemaFor)

  implicit val TimestampEncoder: Encoder[Timestamp] = InstantEncoder.comap[Timestamp](_.toInstant)

  implicit val DateEncoder: Encoder[Date] = LocalDateEncoder.comap[Date](_.toLocalDate)

  implicit val LocalDateTimeEncoder: Encoder[LocalDateTime] = new LocalDateTimeEncoder(SchemaFor.LocalDateTimeSchemaFor)

  private final class LocalDateTimeEncoder(val schemaFor: SchemaFor[LocalDateTime])
      extends TemporalWithLogicalTypeEncoder[LocalDateTime] {
    def epochMillis(temporal: LocalDateTime): Long = temporal.toInstant(ZoneOffset.UTC).toEpochMilli

    def epochSeconds(temporal: LocalDateTime): Long = temporal.toEpochSecond(ZoneOffset.UTC)

    def nanos(temporal: LocalDateTime): Long = temporal.getNano.toLong

    override def withSchema(schemaFor: SchemaFor[LocalDateTime]): Encoder[LocalDateTime] =
      new LocalDateTimeEncoder(schemaFor)
  }

  implicit object OffsetDateTimeEncoder extends Encoder[OffsetDateTime] {
    val schemaFor: SchemaFor[OffsetDateTime] = SchemaFor.OffsetDateTimeSchemaFor
    override def encode(value: OffsetDateTime) = value.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }
}

trait TemporalDecoders {
  implicit val InstantDecoder: Decoder[Instant] = new InstantDecoder(SchemaFor.InstantSchemaFor)

  private final class InstantDecoder(val schemaFor: SchemaFor[Instant])
      extends TemporalWithLogicalTypeDecoder[Instant] {
    def ofEpochMillis(millis: Long): Instant = Instant.ofEpochMilli(millis)

    def ofEpochSeconds(seconds: Long, nanos: Long): Instant = Instant.ofEpochSecond(seconds, nanos)

    override def withSchema(schemaFor: SchemaFor[Instant]): Decoder[Instant] = new InstantDecoder(schemaFor)
  }

  implicit val LocalTimeDecoder: Decoder[LocalTime] = new Decoder[LocalTime] {
    val schemaFor: SchemaFor[LocalTime] = SchemaFor.LocalTimeSchemaFor

    def decode(value: Any): LocalTime = schema.getLogicalType match {
      case _: TimeMillis =>
        value match {
          case l: Long => LocalTime.ofNanoOfDay(l * 1000000L)
          case i: Int  => LocalTime.ofNanoOfDay(i.toLong * 1000000L)
        }
      case _: TimeMicros =>
        value match {
          case l: Long => LocalTime.ofNanoOfDay(l * 1000L)
          case i: Int  => LocalTime.ofNanoOfDay(i.toLong * 1000L)
        }
    }
  }

  implicit val LocalDateDecoder: Decoder[LocalDate] =
    Decoder.IntDecoder.map[LocalDate](i => LocalDate.ofEpochDay(i.toLong)).withSchema(SchemaFor.LocalDateSchemaFor)

  implicit val TimestampDecoder: Decoder[Timestamp] = InstantDecoder.map[Timestamp](Timestamp.from)

  implicit val DateDecoder: Decoder[Date] = LocalDateDecoder.map[Date](Date.valueOf)

  implicit val LocalDateTimeDecoder: Decoder[LocalDateTime] = new LocalDateTimeDecoder(SchemaFor.LocalDateTimeSchemaFor)

  private final class LocalDateTimeDecoder(val schemaFor: SchemaFor[LocalDateTime])
      extends TemporalWithLogicalTypeDecoder[LocalDateTime] {

    def ofEpochMillis(millis: Long): LocalDateTime =
      LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC)

    def ofEpochSeconds(seconds: Long, nanos: Long): LocalDateTime =
      LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), ZoneOffset.UTC)

    override def withSchema(schemaFor: SchemaFor[LocalDateTime]): Decoder[LocalDateTime] =
      new LocalDateTimeDecoder(schemaFor)
  }

  implicit object OffsetDateTimeDecoder extends Decoder[OffsetDateTime] {

    val schemaFor: SchemaFor[OffsetDateTime] = SchemaFor.OffsetDateTimeSchemaFor

    def decode(value: Any): OffsetDateTime =
      OffsetDateTime.parse(value.toString, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }
}

object Temporals {

  private[avro4s] abstract class TemporalWithLogicalTypeEncoder[T] extends Encoder[T] {

    def epochMillis(temporal: T): Long

    def epochSeconds(temporal: T): Long

    def nanos(temporal: T): Long

    val encoder: T => Long = schemaFor.schema.getLogicalType match {
      case _: TimestampMillis => epochMillis
      case _: TimestampMicros =>
        t =>
          epochSeconds(t) * 1000000L + nanos(t) / 1000L
      case TimestampNanosLogicalType =>
        t =>
          epochSeconds(t) * 1000000000L + nanos(t)
      case _ => throw new Avro4sConfigurationException(s"Unsupported logical type for temporal: ${schemaFor.schema}")
    }

    def encode(t: T): AnyRef = java.lang.Long.valueOf(encoder(t))
  }

  private[avro4s] abstract class TemporalWithLogicalTypeDecoder[T] extends Decoder[T] {

    def ofEpochMillis(millis: Long): T

    def ofEpochSeconds(seconds: Long, nanos: Long): T

    val decoder: Any => T = schema.getLogicalType match {
      case _: TimestampMillis => {
        case l: Long => ofEpochMillis(l)
        case i: Int  => ofEpochMillis(i.toLong)
      }

      case _: TimestampMicros => {
        case l: Long => ofEpochSeconds(l / 1000000, l % 1000000 * 1000)
        case i: Int  => ofEpochSeconds(i / 1000000, i % 1000000 * 1000)
      }

      case TimestampNanosLogicalType => {
        case l: Long => ofEpochSeconds(l / 1000000000, l % 1000000000)
        case other =>
          throw new Avro4sConfigurationException(s"Unsupported type for timestamp nanos ${other.getClass.getName}")
      }

      case _ => throw new Avro4sConfigurationException(s"Unsupported logical type for temporal: ${schemaFor.schema}")
    }

    def decode(value: Any): T = decoder(value)
  }
}
