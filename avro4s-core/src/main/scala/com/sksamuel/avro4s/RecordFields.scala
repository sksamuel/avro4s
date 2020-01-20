package com.sksamuel.avro4s

import magnolia.Param
import org.apache.avro.Schema.Field
import org.apache.avro.generic.IndexedRecord

import scala.util.control.NonFatal
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

  class RecordFieldCodec[T](param: Param[Codec, T], field: Option[Field])
      extends RecordFieldBase[Codec, T](param, field, (e: Codec[_]) => e.schema) with FieldCodec[T] {

    def encodeFieldValue(value: T): AnyRef = encodeFieldValue(typeclass, value)

    def decodeFieldValue(record: IndexedRecord): Any = decodeFieldValue(typeclass, record)
  }

  class RecordFieldDecoder[T](param: Param[DecoderV2, T], field: Option[Field])
      extends RecordFieldBase[DecoderV2, T](param, field, (e: DecoderV2[_]) => e.schema) with FieldDecoder {

    def decodeFieldValue(record: IndexedRecord): Any = decodeFieldValue(typeclass, record)
  }

  class RecordFieldEncoder[T](param: Param[EncoderV2, T], field: Option[Field])
      extends RecordFieldBase[EncoderV2, T](param, field, (e: EncoderV2[_]) => e.schema) with FieldEncoder[T] {

    def encodeFieldValue(value: T): AnyRef = encodeFieldValue(typeclass, value)
  }

  /**
    * Workhorse class for field encoder / decoder / codec.
    *
    * Defines how fields are encoded / decoded, and how namespaces or schema overrides are passed to the
    * underlying encoder / decoder / codec that were discovered via Magnolia derivation.
    *
    * @tparam Typeclass typeclass (i.e. Codec[_], Encoder[_], Decoder[_])
    * @tparam T type of the parent record / case class, needed for Magnolia's Param[_, _]
    */
  abstract class RecordFieldBase[Typeclass[X] <: SchemaAware[Typeclass, X], T](val param: Param[Typeclass, T],
                                                                               val field: Option[Field],
                                                                               sf: Typeclass[_] => Schema) {

    // Propagate of namespaces and schema changes. Schema changes have precedence over namespace changes, as
    // schema changes (via .withSchema) are more flexible than namespace changes (via param annotations).
    protected val typeclass: Typeclass[param.PType] = {
      val namespace = new AnnotationExtractors(param.annotations).namespace
      (param.typeclass, namespace, field.map(_.schema)) match {
        case (typeclass, _, Some(s)) if s.getType != sf(typeclass).getType =>
          typeclass.withSchema(SchemaForV2[param.PType](s))
        case (m: NamespaceAware[Typeclass[param.PType]] @unchecked, Some(ns), _) => m.withNamespace(ns)
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
