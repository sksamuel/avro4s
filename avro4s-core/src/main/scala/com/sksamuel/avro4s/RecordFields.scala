package com.sksamuel.avro4s

import magnolia.Param
import org.apache.avro.Schema.Field
import org.apache.avro.generic.IndexedRecord

import scala.util.control.NonFatal
import org.apache.avro.Schema

object RecordFields {

  class RecordFieldDecoder[T](param: Param[Decoder, T], field: Option[Field])
      extends RecordFieldBase[Decoder, T](param, field, param.typeclass.schema)
      with Serializable {

    private val fieldPosition = field.map(_.pos).getOrElse(-1)

    def fastDecodeFieldValue(record: IndexedRecord): Any =
      if (fieldPosition == -1) defaultFieldValue
      else tryDecode(record.get(fieldPosition))

    def safeDecodeFieldValue(record: IndexedRecord): Any =
      if (fieldPosition == -1) defaultFieldValue
      else {
        val schemaField = record.getSchema.getField(field.get.name)
        if (schemaField == null) defaultFieldValue else tryDecode(record.get(schemaField.pos))
      }

    @inline
    private def defaultFieldValue: Any = param.default match {
      case Some(default) => default
      // there is no default, so the field must be an option
      case None => typeclass.decode(null)
    }

    @inline
    private def tryDecode(value: Any): Any =
      try {
        typeclass.decode(value)
      } catch {
        case NonFatal(ex) => param.default.getOrElse(throw ex)
      }

  }

  class RecordFieldEncoder[T](p: Param[Encoder, T], field: Option[Field])
      extends RecordFieldBase[Encoder, T](p, field, p.typeclass.schema)
      with Serializable {

    def encodeFieldValue(value: T): AnyRef = typeclass.encode(param.dereference(value))
  }

  /**
    * Base class for field encoder / decoder.
    *
    * Defines how fields are encoded / decoded, and how namespaces or schema overrides are passed to the
    * underlying encoder / decoder that were discovered via Magnolia derivation.
    *
    * @tparam Typeclass typeclass (i.e. Encoder[_], Decoder[_])
    * @tparam T type of the parent record / case class, needed for Magnolia's Param[_, _]
    */
  abstract class RecordFieldBase[Typeclass[X] <: SchemaAware[Typeclass, X], T](val param: Param[Typeclass, T],
                                                                               val field: Option[Field],
                                                                               derivedSchema: Schema) {

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
        case (tc, _, Some(s)) if typesDiffer(s, derivedSchema)                   => tc.withSchema(SchemaFor[param.PType](s))
        case (m: NamespaceAware[Typeclass[param.PType]] @unchecked, Some(ns), _) => m.withNamespace(ns)
        case (tc, _, _)                                                          => tc
      }
    }
  }
}
