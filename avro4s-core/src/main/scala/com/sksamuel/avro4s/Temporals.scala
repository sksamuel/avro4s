//package com.sksamuel.avro4s
//
//import java.sql.{Date, Timestamp}
//import java.time._
//import java.time.format.DateTimeFormatter
//
//import com.sksamuel.avro4s.SchemaFor.TimestampNanosLogicalType
//import com.sksamuel.avro4s.Temporals.{TemporalWithLogicalTypeDecoder, TemporalWithLogicalTypeEncoder}
//import org.apache.avro.LogicalTypes.{TimeMicros, TimeMillis, TimestampMicros, TimestampMillis}
//

//
//trait TemporalDecoders {
//  implicit val InstantDecoder: Decoder[Instant] = new InstantDecoder(SchemaFor.InstantSchemaFor)
//
//  private final class InstantDecoder(val schemaFor: SchemaFor[Instant])
//      extends TemporalWithLogicalTypeDecoder[Instant] {
//    def ofEpochMillis(millis: Long): Instant = Instant.ofEpochMilli(millis)
//
//    def ofEpochSeconds(seconds: Long, nanos: Long): Instant = Instant.ofEpochSecond(seconds, nanos)
//
//    override def withSchema(schemaFor: SchemaFor[Instant]): Decoder[Instant] = new InstantDecoder(schemaFor)
//  }
//
//  implicit val LocalTimeDecoder: Decoder[LocalTime] = new LocalTimeDecoder(SchemaFor.LocalTimeSchemaFor)
//
//  private final class LocalTimeDecoder(val schemaFor: SchemaFor[LocalTime]) extends Decoder[LocalTime] {
//
//    val toNanosFactor = schema.getLogicalType match {
//      case _: TimeMicros => 1000L
//      case _: TimeMillis => 1000000L
//      case _             => throw new Avro4sConfigurationException(s"Unsupported logical type for LocalTime: ${schemaFor.schema}")
//    }
//
//    def decode(value: Any): LocalTime = value match {
//      case l: Long => LocalTime.ofNanoOfDay(l * toNanosFactor)
//      case i: Int  => LocalTime.ofNanoOfDay(i.toLong * toNanosFactor)
//    }
//
//    override def withSchema(schemaFor: SchemaFor[LocalTime]): Decoder[LocalTime] = new LocalTimeDecoder(schemaFor)
//  }
//
//  implicit val LocalDateDecoder: Decoder[LocalDate] =
//    Decoder.IntDecoder.map[LocalDate](i => LocalDate.ofEpochDay(i.toLong)).withSchema(SchemaFor.LocalDateSchemaFor)
//
//  implicit val TimestampDecoder: Decoder[Timestamp] = InstantDecoder.map[Timestamp](Timestamp.from)
//
//  implicit val DateDecoder: Decoder[Date] = LocalDateDecoder.map[Date](Date.valueOf)
//
//  implicit val LocalDateTimeDecoder: Decoder[LocalDateTime] = new LocalDateTimeDecoder(SchemaFor.LocalDateTimeSchemaFor)
//
//  private final class LocalDateTimeDecoder(val schemaFor: SchemaFor[LocalDateTime])
//      extends TemporalWithLogicalTypeDecoder[LocalDateTime] {
//
//    def ofEpochMillis(millis: Long): LocalDateTime =
//      LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC)
//
//    def ofEpochSeconds(seconds: Long, nanos: Long): LocalDateTime =
//      LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), ZoneOffset.UTC)
//
//    override def withSchema(schemaFor: SchemaFor[LocalDateTime]): Decoder[LocalDateTime] =
//      new LocalDateTimeDecoder(schemaFor)
//  }
//
//  implicit object OffsetDateTimeDecoder extends Decoder[OffsetDateTime] {
//
//    val schemaFor: SchemaFor[OffsetDateTime] = SchemaFor.OffsetDateTimeSchemaFor
//
//    def decode(value: Any): OffsetDateTime =
//      OffsetDateTime.parse(value.toString, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
//  }
//}
//
//object Temporals {
//

//
//  private[avro4s] abstract class TemporalWithLogicalTypeDecoder[T] extends Decoder[T] {
//
//    def ofEpochMillis(millis: Long): T
//
//    def ofEpochSeconds(seconds: Long, nanos: Long): T
//
//    val decoder: Any => T = schema.getLogicalType match {
//      case _: TimestampMillis => {
//        case l: Long => ofEpochMillis(l)
//        case i: Int  => ofEpochMillis(i.toLong)
//      }
//
//      case _: TimestampMicros => {
//        case l: Long => ofEpochSeconds(l / 1000000, l % 1000000 * 1000)
//        case i: Int  => ofEpochSeconds(i / 1000000, i % 1000000 * 1000)
//      }
//
//      case TimestampNanosLogicalType => {
//        case l: Long => ofEpochSeconds(l / 1000000000, l % 1000000000)
//        case other =>
//          throw new Avro4sConfigurationException(s"Unsupported type for timestamp nanos ${other.getClass.getName}")
//      }
//
//      case _ => throw new Avro4sConfigurationException(s"Unsupported logical type for temporal: ${schemaFor.schema}")
//    }
//
//    def decode(value: Any): T = decoder(value)
//  }
//}
