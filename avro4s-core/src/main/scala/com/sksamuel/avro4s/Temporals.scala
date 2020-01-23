package com.sksamuel.avro4s

import java.sql.{Date, Timestamp}
import java.time._

import com.sksamuel.avro4s.SchemaFor.TimestampNanosLogicalType
import org.apache.avro.LogicalTypes.{TimeMicros, TimeMillis, TimestampMicros, TimestampMillis}
import org.apache.avro.{Schema, SchemaBuilder}

trait TemporalCodecs {
  implicit val InstantCodec: Codec[Instant] = Temporals.InstantCodec
  implicit val LocalTimeCodec: Codec[LocalTime] = Temporals.LocalTimeCodec
  implicit val LocalDateCodec: Codec[LocalDate] = Temporals.LocalDateCodec
  implicit val TimestampCodec: Codec[Timestamp] = Temporals.TimestampCodec
  implicit val DateCodec: Codec[Date] = Temporals.DateCodec
  implicit val LocalDateTimeCodec: Codec[LocalDateTime] = Temporals.LocalDateTimeCodec
}

trait TemporalEncoders {
  implicit val InstantEncoder: EncoderV2[Instant] = Temporals.InstantCodec
  implicit val LocalTimeEncoder: EncoderV2[LocalTime] = Temporals.LocalTimeCodec
  implicit val LocalDateEncoder: EncoderV2[LocalDate] = Temporals.LocalDateCodec
  implicit val TimestampEncoder: EncoderV2[Timestamp] = Temporals.TimestampCodec
  implicit val DateEncoder: EncoderV2[Date] = Temporals.DateCodec
  implicit val LocalDateTimeEncoder: EncoderV2[LocalDateTime] = Temporals.LocalDateTimeCodec
}

trait TemporalDecoders {
  implicit val InstantDecoder: DecoderV2[Instant] = Temporals.InstantCodec
  implicit val LocalTimeDecoder: DecoderV2[LocalTime] = Temporals.LocalTimeCodec
  implicit val LocalDateDecoder: DecoderV2[LocalDate] = Temporals.LocalDateCodec
  implicit val TimestampDecoder: DecoderV2[Timestamp] = Temporals.TimestampCodec
  implicit val DateDecoder: DecoderV2[Date] = Temporals.DateCodec
  implicit val LocalDateTimeDecoder: DecoderV2[LocalDateTime] = Temporals.LocalDateTimeCodec
}

object Temporals {

  val InstantCodec =
    BaseTypes.LongCodec.inmap[Instant](Instant.ofEpochMilli, _.toEpochMilli).withSchema(SchemaForV2.InstantSchema)

  val LocalTimeCodec: Codec[LocalTime] = new Codec[LocalTime] {
    val schema: Schema = SchemaForV2.LocalTimeSchema.schema

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
      .withSchema(SchemaForV2.LocalDateSchema)

  val TimestampCodec: Codec[Timestamp] = InstantCodec.inmap[Timestamp](Timestamp.from, _.toInstant)

  val DateCodec: Codec[Date] = LocalDateCodec.inmap[Date](Date.valueOf, _.toLocalDate)

  val LocalDateTimeCodec: Codec[LocalDateTime] = new LocalDateTimeCodec(
    TimestampNanosLogicalType.addToSchema(SchemaBuilder.builder.longType))

  class LocalDateTimeCodec(val schema: Schema) extends Codec[LocalDateTime] {

    def encode(t: LocalDateTime): AnyRef = {
      val long = schema.getLogicalType match {
        case _: TimestampMillis        => t.toInstant(ZoneOffset.UTC).toEpochMilli
        case _: TimestampMicros        => t.toEpochSecond(ZoneOffset.UTC) * 1000000L + t.getNano.toLong / 1000L
        case TimestampNanosLogicalType => t.toEpochSecond(ZoneOffset.UTC) * 1000000000L + t.getNano.toLong
        case _                         => sys.error(s"Unsupported type for LocalDateTime: $schema")
      }
      java.lang.Long.valueOf(long)
    }

    def decode(value: Any): LocalDateTime = {
      schema.getLogicalType match {
        case _: TimestampMillis =>
          value match {
            case i: Int  => LocalDateTime.ofInstant(Instant.ofEpochMilli(i.toLong), ZoneOffset.UTC)
            case l: Long => LocalDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneOffset.UTC)
          }

        case _: TimestampMicros =>
          value match {
            case i: Int =>
              LocalDateTime.ofInstant(Instant.ofEpochMilli(i / 1000), ZoneOffset.UTC).plusNanos(i % 1000 * 1000)
            case l: Long =>
              LocalDateTime.ofInstant(Instant.ofEpochMilli(l / 1000), ZoneOffset.UTC).plusNanos(l % 1000 * 1000)
          }

        case TimestampNanosLogicalType =>
          value match {
            case l: Long =>
              val nanos = l % 1000000
              LocalDateTime.ofInstant(Instant.ofEpochMilli(l / 1000000), ZoneOffset.UTC).plusNanos(nanos)
            case other => sys.error(s"Unsupported type for timestamp nanos ${other.getClass.getName}")
          }
      }
    }

    override def withSchema(schemaFor: SchemaForV2[LocalDateTime]): Codec[LocalDateTime] =
      new LocalDateTimeCodec(schemaFor.schema)
  }
}
