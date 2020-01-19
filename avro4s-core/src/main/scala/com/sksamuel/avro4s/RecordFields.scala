package com.sksamuel.avro4s

import magnolia.Param
import org.apache.avro.Schema.Field
import org.apache.avro.generic.IndexedRecord

import scala.util.control.NonFatal
import com.sksamuel.avro4s.Codec.{Typeclass => CodecTC}
import com.sksamuel.avro4s.DecoderV2.{Typeclass => DecoderTC}
import com.sksamuel.avro4s.EncoderV2.{Typeclass => EncoderTC}
import org.apache.avro.Schema

object RecordFields {

  trait FieldEncoder[T] {

    def field: Option[Field]

    def encodeFieldValue(value: T): AnyRef
  }

  trait FieldDecoder {
    def decodeFieldValue(record: IndexedRecord): Any
  }

  trait FieldCodec[T] extends FieldEncoder[T] with FieldDecoder

  class RecordFieldCodec[T](param: Param[CodecTC, T], field: Option[Field])
      extends RecordFieldBase[CodecTC, T](param, field, (e: Codec[_]) => e.schema) with FieldCodec[T] {

    def encodeFieldValue(value: T): AnyRef = encodeFieldValue(typeclass, value)

    def decodeFieldValue(record: IndexedRecord): Any = decodeFieldValue(typeclass, record)
  }

  class RecordFieldDecoder[T](param: Param[DecoderTC, T], field: Option[Field])
      extends RecordFieldBase[DecoderTC, T](param, field, (e: DecoderV2[_]) => e.schema) with FieldDecoder {

    def decodeFieldValue(record: IndexedRecord): Any = decodeFieldValue(typeclass, record)
  }

  class RecordFieldEncoder[T](param: Param[EncoderTC, T], field: Option[Field])
      extends RecordFieldBase[EncoderTC, T](param, field, (e: EncoderV2[_]) => e.schema) with FieldEncoder[T] {

    def encodeFieldValue(value: T): AnyRef = encodeFieldValue(typeclass, value)
  }

  /**
    * Workhorse class for field encoder / decoder / codec.
    *
    * Defines how fields are encoded / decoded, and how namespaces or schema overrides are passed to the
    * underlying encoder / decoder / codec that were discovered via Magnolia derivation.
    *
    * @tparam TC typeclass (i.e. Codec[_], Encoder[_], Decoder[_])
    * @tparam T type of the parent record / case class, needed for Magnolia's Param[_, _]
    */
  class RecordFieldBase[TC[X] <: SchemaAware[TC, X], T](val param: Param[TC, T],
                                                        val field: Option[Field],
                                                        sf: TC[_] => Schema) {

    // Propagate of namespaces and schema changes. Schema changes have precedence over namespace changes, as
    // schema changes (via .withSchema) are more flexible than namespace changes (via param annotations).
    protected val typeclass: TC[param.PType] = {
      val namespace = new AnnotationExtractors(param.annotations).namespace
      (param.typeclass, namespace, field.map(_.schema)) match {
        case (typeclass, _, Some(s)) if s.getType != sf(typeclass).getType =>
          typeclass.withSchema(SchemaForV2[param.PType](s))
        case (m: NamespaceAware[TC[param.PType]] @unchecked, Some(ns), _) => m.withNamespace(ns)
        case (codec, _, _)                                                => codec
      }
    }

    private val fieldPosition = field.map(_.pos).getOrElse(-1)

    protected def encodeFieldValue(encoder: EncoderV2[param.PType], value: T): AnyRef =
      encoder.encode(param.dereference(value))

    protected def decodeFieldValue(decoder: DecoderV2[param.PType], record: IndexedRecord): param.PType =
      if (fieldPosition == -1) {
        param.default match {
          case Some(default) => default
          // there is no default, so the field must be an option
          case None => decoder.decode(null)
        }
      } else {
        val value = record.get(fieldPosition)
        try {
          decoder.decode(value)
        } catch {
          case NonFatal(ex) => param.default.getOrElse(throw ex)
        }
      }
  }
}
