package com.sksamuel.avro4s

import java.sql.{Date, Timestamp}
import java.time._
import java.time.format.DateTimeFormatter

import com.sksamuel.avro4s.SchemaFor.TimestampNanosLogicalType
import org.apache.avro.LogicalTypes.{TimeMicros, TimeMillis, TimestampMicros, TimestampMillis}

trait TemporalEncoders {
  implicit val InstantEncoder: Encoder[Instant] = Temporals.InstantCodec
  implicit val LocalTimeEncoder: Encoder[LocalTime] = Temporals.LocalTimeCodec
  implicit val LocalDateEncoder: Encoder[LocalDate] = Temporals.LocalDateCodec
  implicit val TimestampEncoder: Encoder[Timestamp] = Temporals.TimestampCodec
  implicit val DateEncoder: Encoder[Date] = Temporals.DateCodec
  implicit val LocalDateTimeEncoder: Encoder[LocalDateTime] = Temporals.LocalDateTimeCodec
  implicit val OffsetDateTimeEncoder: Encoder[OffsetDateTime] = Temporals.OffsetDateTimeCodec
}

trait TemporalDecoders {
  implicit val InstantDecoder: Decoder[Instant] = Temporals.InstantCodec
  implicit val LocalTimeDecoder: Decoder[LocalTime] = Temporals.LocalTimeCodec
  implicit val LocalDateDecoder: Decoder[LocalDate] = Temporals.LocalDateCodec
  implicit val TimestampDecoder: Decoder[Timestamp] = Temporals.TimestampCodec
  implicit val DateDecoder: Decoder[Date] = Temporals.DateCodec
  implicit val LocalDateTimeDecoder: Decoder[LocalDateTime] = Temporals.LocalDateTimeCodec
  implicit val OffsetDateTimeDecoder: Decoder[OffsetDateTime] = Temporals.OffsetDateTimeCodec
}

object Temporals {

  val InstantCodec =
    BaseTypes.LongCodec.inmap[Instant](Instant.ofEpochMilli, _.toEpochMilli).withSchema(SchemaFor.InstantSchema)

  val LocalTimeCodec: Codec[LocalTime] = new Codec[LocalTime] {
    val schemaFor: SchemaFor[LocalTime] = SchemaFor.LocalTimeSchema

    def encode(value: LocalTime): AnyRef = java.lang.Long.valueOf(value.toNanoOfDay / 1000)

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

  val LocalDateCodec: Codec[LocalDate] =
    BaseTypes.IntCodec
      .inmap[LocalDate](i => LocalDate.ofEpochDay(i.toLong), _.toEpochDay.toInt)
      .withSchema(SchemaFor.LocalDateSchema)

  val TimestampCodec: Codec[Timestamp] = InstantCodec.inmap[Timestamp](Timestamp.from, _.toInstant)

  val DateCodec: Codec[Date] = LocalDateCodec.inmap[Date](Date.valueOf, _.toLocalDate)

  val LocalDateTimeCodec: Codec[LocalDateTime] = new LocalDateTimeCodec(SchemaFor.LocalDateTimeSchema)

  class LocalDateTimeCodec(val schemaFor: SchemaFor[LocalDateTime]) extends Codec[LocalDateTime] {

    val encoder: LocalDateTime => Long = schemaFor.schema.getLogicalType match {
      case _: TimestampMillis => _.toInstant(ZoneOffset.UTC).toEpochMilli
      case _: TimestampMicros =>
        t =>
          t.toEpochSecond(ZoneOffset.UTC) * 1000000L + t.getNano.toLong / 1000L
      case TimestampNanosLogicalType =>
        t =>
          t.toEpochSecond(ZoneOffset.UTC) * 1000000000L + t.getNano.toLong
      case _ => sys.error(s"Unsupported type for LocalDateTime: ${schemaFor.schema}")
    }

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
        case other => sys.error(s"Unsupported type for timestamp nanos ${other.getClass.getName}")
      }
    }

    def encode(t: LocalDateTime): AnyRef = java.lang.Long.valueOf(encoder(t))

    def decode(value: Any): LocalDateTime = decoder(value)

    override def withSchema(schemaFor: SchemaFor[LocalDateTime]): Codec[LocalDateTime] =
      new LocalDateTimeCodec(schemaFor)
  }

  object OffsetDateTimeCodec extends Codec[OffsetDateTime] {

    val schemaFor: SchemaFor[OffsetDateTime] = SchemaFor.OffsetDateTimeSchema

    def decode(value: Any): OffsetDateTime =
      OffsetDateTime.parse(value.toString, DateTimeFormatter.ISO_OFFSET_DATE_TIME)

    override def encode(value: OffsetDateTime) =
      value.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }

}
