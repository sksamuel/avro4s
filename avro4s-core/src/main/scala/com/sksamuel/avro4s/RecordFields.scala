package com.sksamuel.avro4s

import magnolia.Param
import org.apache.avro.Schema.Field
import org.apache.avro.generic.IndexedRecord

import scala.util.control.NonFatal
import org.apache.avro.Schema

object RecordFields {

  trait FieldEncoder[T] extends Serializable {

    def field: Option[Field]

    def encodeFieldValue(value: T): AnyRef
  }

  trait FieldDecoder extends Serializable {
    def fastDecodeFieldValue(record: IndexedRecord): Any

    def safeDecodeFieldValue(record: IndexedRecord): Any
  }

  trait FieldCodec[T] extends FieldEncoder[T] with FieldDecoder

  class RecordFieldCodec[T](param: Param[Codec, T], field: Option[Field])
      extends RecordFieldBase[Codec, T](param, field, (e: Codec[_]) => e.schema)
      with FieldCodec[T] {

    def encodeFieldValue(value: T): AnyRef = fastEncodeFieldValue(typeclass, value)

    def fastDecodeFieldValue(record: IndexedRecord): Any = fastDecodeFieldValue(typeclass, record)

    def safeDecodeFieldValue(record: IndexedRecord): Any = safeDecodeFieldValue(typeclass, record)
  }

  class RecordFieldDecoder[T](param: Param[DecoderV2, T], field: Option[Field])
      extends RecordFieldBase[DecoderV2, T](param, field, (e: DecoderV2[_]) => e.schema)
      with FieldDecoder {

    def fastDecodeFieldValue(record: IndexedRecord): Any = fastDecodeFieldValue(typeclass, record)

    def safeDecodeFieldValue(record: IndexedRecord): Any = safeDecodeFieldValue(typeclass, record)
  }

  class RecordFieldEncoder[T](param: Param[EncoderV2, T], field: Option[Field])
      extends RecordFieldBase[EncoderV2, T](param, field, (e: EncoderV2[_]) => e.schema)
      with FieldEncoder[T] {

    def encodeFieldValue(value: T): AnyRef = fastEncodeFieldValue(typeclass, value)
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

    private def typesDiffer(schemaA: Schema, schemaB: Schema): Boolean =
      schemaA.getType != schemaB.getType || schemaA.getLogicalType != schemaB.getLogicalType || fieldsDiffer(schemaA,
                                                                                                             schemaB)

    private def fieldsDiffer(schemaA: Schema, schemaB: Schema): Boolean =
      schemaA.getType == Schema.Type.RECORD && schemaA.getFields != schemaB.getFields

    // Propagate of namespaces and schema changes. Schema changes have precedence over namespace changes, as
    // schema changes (via .withSchema) are more flexible than namespace changes (via param annotations).
    protected val typeclass: Typeclass[param.PType] = {
      val namespace = new AnnotationExtractors(param.annotations).namespace
      (param.typeclass, namespace, field.map(_.schema)) match {
        case (typeclass, _, Some(s)) if typesDiffer(s, sf(typeclass)) =>
          typeclass.withSchema(SchemaForV2[param.PType](s))
        case (m: NamespaceAware[Typeclass[param.PType]] @unchecked, Some(ns), _) => m.withNamespace(ns)
        case (codec, _, _)                                                       => codec
      }
    }

    private val fieldPosition = field.map(_.pos).getOrElse(-1)

    @inline
    protected final def fastEncodeFieldValue(encoder: EncoderV2[param.PType], value: T): AnyRef =
      encoder.encode(param.dereference(value))

    @inline
    protected final def fastDecodeFieldValue(decoder: DecoderV2[param.PType], record: IndexedRecord): param.PType =
      if (fieldPosition == -1) defaultFieldValue(decoder)
      else tryDecode(decoder, record.get(fieldPosition))

    @inline
    protected final def safeDecodeFieldValue(decoder: DecoderV2[param.PType], record: IndexedRecord): param.PType =
      if (fieldPosition == -1) defaultFieldValue(decoder)
      else {
        val schemaField = record.getSchema.getField(field.get.name)
        if (schemaField == null) defaultFieldValue(decoder) else tryDecode(decoder, record.get(schemaField.pos))
      }

    @inline
    private def defaultFieldValue(decoder: DecoderV2[param.PType]) = param.default match {
      case Some(default) => default
      // there is no default, so the field must be an option
      case None => decoder.decode(null)
    }

    @inline
    private def tryDecode(decoder: DecoderV2[param.PType], value: Any) =
      try {
        decoder.decode(value)
      } catch {
        case NonFatal(ex) => param.default.getOrElse(throw ex)
      }
  }
}
