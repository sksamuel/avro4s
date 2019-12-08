package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.sql.{ Date, Timestamp }
import java.time.{ Instant, LocalDate, LocalDateTime, LocalTime, OffsetDateTime }
import java.util.UUID

import scala.reflect.runtime.universe._
import org.apache.avro.Schema

import scala.reflect.ClassTag

trait SchemaCodec[T] { self =>
  def schemaFor: SchemaFor[T]
  def encoder: Encoder[T]
  def decoder: Decoder[T]

  // keeps the underlying avro schema and applies these transformations in memory
  def invariantTransform[A](f: T => A)(g: A => T): SchemaCodec[A] =
    new SchemaCodec[A] {

      override def schemaFor: SchemaFor[A] = new SchemaFor[A] {

        override def schema(fieldMapper: FieldMapper): Schema =
          self.schemaFor.schema(fieldMapper)
      }

      override def encoder: Encoder[A] = self.encoder.comap(g)

      override def decoder: Decoder[A] = self.decoder.map(f)
    }
}

object SchemaCodec extends SchemaCodecProductTypes {

  def anyVal[Wrapper <: AnyVal: Encoder: Decoder](implicit schemaFor: SchemaFor[Wrapper]) =
    fromSchemaFor(schemaFor)

  /**
    * Definitions from avro4s.
    * Iterable, Coproduct and Tuple are implemented in avro4s but not here.
    */
  implicit val StringSchemaCodec: SchemaCodec[String] = fromSchemaFor(SchemaFor.StringSchemaFor)
  implicit val LongSchemaCodec: SchemaCodec[Long] = fromSchemaFor(SchemaFor.LongSchemaFor)
  implicit val IntSchemaCodec: SchemaCodec[Int] = fromSchemaFor(SchemaFor.IntSchemaFor)
  implicit val DoubleSchemaCodec: SchemaCodec[Double] = fromSchemaFor(SchemaFor.DoubleSchemaFor)
  implicit val FloatSchemaCodec: SchemaCodec[Float] = fromSchemaFor(SchemaFor.FloatSchemaFor)

  implicit val BooleanSchemaCodec: SchemaCodec[Boolean] = fromSchemaFor(
    SchemaFor.BooleanSchemaFor
  )

  implicit val ByteArraySchemaCodec: SchemaCodec[Array[Byte]] = fromSchemaFor(
    SchemaFor.ByteArraySchemaFor
  )

  implicit val ByteSeqSchemaCodec: SchemaCodec[Seq[Byte]] = fromSchemaFor(
    SchemaFor.ByteSeqSchemaFor
  )

  implicit val ByteListSchemaCodec: SchemaCodec[List[Byte]] = fromSchemaFor(
    SchemaFor.ByteListSchemaFor
  )

  implicit val ByteVectorSchemaCodec: SchemaCodec[Vector[Byte]] = fromSchemaFor(
    SchemaFor.ByteVectorSchemaFor
  )

  implicit val ByteBufferSchemaCodec: SchemaCodec[ByteBuffer] = fromSchemaFor(
    SchemaFor.ByteBufferSchemaFor
  )
  implicit val ShortSchemaCodec: SchemaCodec[Short] = fromSchemaFor(SchemaFor.ShortSchemaFor)
  implicit val ByteSchemaCodec: SchemaCodec[Byte] = fromSchemaFor(SchemaFor.ByteSchemaFor)

  implicit val UUIDSchemaCodec: SchemaCodec[UUID] = fromSchemaFor(SchemaFor.UUIDSchemaFor)

  implicit def mapSchemaCodec[V](
                                  implicit schemaCodec: SchemaCodec[V]
                                ): SchemaCodec[Map[String, V]] =
    fromSchemaFor(SchemaFor.mapSchemaFor(schemaCodec.schemaFor))(
      Encoder.mapEncoder(schemaCodec.encoder),
      Decoder.mapDecoder(schemaCodec.decoder)
    )

  implicit def bigDecimalCodec(
                                implicit sp: ScalePrecision = ScalePrecision.default
                              ): SchemaCodec[BigDecimal] = fromSchemaFor(SchemaFor.bigDecimalFor)

  implicit def eitherSchemaCodec[A: WeakTypeTag: Manifest, B: WeakTypeTag: Manifest](
                                                                                      implicit leftSchemaCodec: SchemaCodec[A],
                                                                                      rightSchemaCodec: SchemaCodec[B]
                                                                                    ): SchemaCodec[Either[A, B]] =
    fromSchemaFor(SchemaFor.eitherSchemaFor(leftSchemaCodec.schemaFor, rightSchemaCodec.schemaFor))(
      Encoder.eitherEncoder(leftSchemaCodec.encoder, rightSchemaCodec.encoder),
      Decoder.eitherDecoder(
        implicitly,
        leftSchemaCodec.decoder,
        implicitly,
        implicitly,
        rightSchemaCodec.decoder,
        implicitly
      )
    )

  implicit def optionSchemaCodec[T](
                                     implicit schemaCodec: SchemaCodec[T]
                                   ): SchemaCodec[Option[T]] =
    fromSchemaFor(SchemaFor.optionSchemaFor(schemaCodec.schemaFor))(
      Encoder.optionEncoder(schemaCodec.encoder),
      Decoder.optionDecoder(schemaCodec.decoder)
    )

  implicit def arraySchemaCodec[S: ClassTag](
                                              implicit schemaCodec: SchemaCodec[S]
                                            ): SchemaCodec[Array[S]] =
    fromSchemaFor(SchemaFor.arraySchemaFor(schemaCodec.schemaFor))(
      Encoder.arrayEncoder(schemaCodec.encoder),
      Decoder.arrayDecoder(schemaCodec.decoder, implicitly)
    )

  implicit def listSchemaCodec[S](
                                   implicit schemaCodec: SchemaCodec[S]
                                 ): SchemaCodec[List[S]] =
    fromSchemaFor(SchemaFor.listSchemaFor(schemaCodec.schemaFor))(
      Encoder.listEncoder(schemaCodec.encoder),
      Decoder.listDecoder(schemaCodec.decoder)
    )

  implicit def setSchemaCodec[S](
                                  implicit schemaCodec: SchemaCodec[S]
                                ): SchemaCodec[Set[S]] =
    fromSchemaFor(SchemaFor.setSchemaFor(schemaCodec.schemaFor))(
      Encoder.setEncoder(schemaCodec.encoder),
      Decoder.setDecoder(schemaCodec.decoder)
    )

  implicit def vectorSchemaCodec[S](
                                     implicit schemaCodec: SchemaCodec[S]
                                   ): SchemaCodec[Vector[S]] =
    fromSchemaFor(SchemaFor.vectorSchemaFor(schemaCodec.schemaFor))(
      Encoder.vectorEncoder(schemaCodec.encoder),
      Decoder.vectorDecoder(schemaCodec.decoder)
    )

  implicit def seqSchemaCodec[S](
                                  implicit schemaCodec: SchemaCodec[S]
                                ): SchemaCodec[Seq[S]] =
    fromSchemaFor(SchemaFor.seqSchemaFor(schemaCodec.schemaFor))(
      Encoder.seqEncoder(schemaCodec.encoder),
      Decoder.seqDecoder(schemaCodec.decoder)
    )

  implicit val TimestampSchemaCodec: SchemaCodec[Timestamp] = fromSchemaFor(
    SchemaFor.TimestampSchemaFor
  )

  implicit val LocalTimeSchemaCodec: SchemaCodec[LocalTime] = fromSchemaFor(
    SchemaFor.LocalTimeSchemaFor
  )

  implicit val LocalDateSchemaCodec: SchemaCodec[LocalDate] = fromSchemaFor(
    SchemaFor.LocalDateSchemaFor
  )

  implicit val LocalDateTimeSchemaCodec: SchemaCodec[LocalDateTime] = fromSchemaFor(
    SchemaFor.LocalDateTimeSchemaFor
  )

  implicit val DateSchemaCodec: SchemaCodec[Date] = fromSchemaFor(SchemaFor.DateSchemaFor)

  implicit val InstantSchemaCodec: SchemaCodec[Instant] = fromSchemaFor(
    SchemaFor.InstantSchemaFor
  )

  implicit val OffsetDateTimeSchemaCodec: SchemaCodec[OffsetDateTime] = fromSchemaFor(
    SchemaFor.OffsetDateTimeSchemaFor
  )

  implicit def javaEnumSchemaCodec[E <: Enum[E]](implicit tag: ClassTag[E]): SchemaCodec[E] =
    fromSchemaFor(SchemaFor.javaEnumSchemaFor)(Encoder.javaEnumEncoder, Decoder.javaEnumDecoder[E])

  implicit def scalaEnumSchemaCodec[E <: scala.Enumeration#Value](
                                                                   implicit tag: TypeTag[E]
                                                                 ): SchemaCodec[E] = fromSchemaFor(SchemaFor.scalaEnumSchemaFor)

  private def fromSchemaFor[T](schema: SchemaFor[T])(implicit _encoder: Encoder[T],
                                                     _decoder: Decoder[T]): SchemaCodec[T] =
    new SchemaCodec[T] {
      override def schemaFor: SchemaFor[T] = schema

      override def encoder: Encoder[T] = _encoder

      override def decoder: Decoder[T] = _decoder
    }
}
