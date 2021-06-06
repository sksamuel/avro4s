//package com.sksamuel.avro4s
//
//import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate, NoUpdate}
//import magnolia.{CaseClass, Param}
//import org.apache.avro.Schema.Field
//import org.apache.avro.generic.IndexedRecord
//
//import scala.util.control.NonFatal
//import org.apache.avro.{Schema, SchemaBuilder}
//
//object RecordFields {
//
//  class FieldEncoder[T](val param: Param[Encoder, T]) extends Serializable {
//    // using inner class here to be able to reference param.PType below, and keep the type relation intact in
//    // the apply() method below.
//    class ValueEncoder(encoder: Encoder[param.PType], val fieldName: String) extends Serializable {
//      def encodeFieldValue(value: T): AnyRef = encoder.encode(param.dereference(value))
//    }
//
//    // using the apply method here to create a ValueEncoder while keeping the types consistent and making sure to
//    // not accidentally capture non-serializable objects as class parameters that are only needed for creating the encoder.
//    def apply(env: DefinitionEnvironment[Encoder],
//              update: SchemaUpdate,
//              record: Schema,
//              ctx: CaseClass[Encoder, T],
//              fieldMapper: FieldMapper): (Field, ValueEncoder) = {
//
//      val (encoder, field) = update match {
//        case FullSchemaUpdate(sf) =>
//          // in a full schema update, the schema is the leading information and we derive encoder modifications from it.
//          // so we extract the field and create a schema update from its schema and apply it to the encoder
//          // via resolveEncoder.
//          val field = extractField(param, sf)
//
//          val fieldUpdate = FullSchemaUpdate(SchemaFor(field.schema(), sf.fieldMapper))
//          val encoder = param.typeclass.resolveEncoder(env, fieldUpdate)
//          (encoder, field)
//
//        case _ =>
//          // Otherwise, we look for annotations on the field (such as AvroFixed or AvroNamespace) and use those to
//          // compute modifications to apply to the encoder.
//          // The field schema is then derived from the encoder schema.
//          val encoder = param.typeclass.resolveEncoder(env, fieldUpdate(param, record, fieldMapper))
//          (encoder, buildField(param, record, ctx, encoder.schema, fieldMapper))
//      }
//
//      field -> new ValueEncoder(encoder, field.name)
//    }
//  }
//
//  class FieldDecoder[T](val param: Param[Decoder, T]) extends Serializable {
//    // using inner class here to be able to reference param.PType below, and keep the type relation intact in
//    // the apply() method below.
//    class ValueDecoder(decoder: Decoder[param.PType], val fieldName: Option[String], fieldPosition: Int)
//        extends Serializable {
//
//      def fastDecodeFieldValue(record: IndexedRecord): Any =
//        if (fieldPosition == -1) defaultFieldValue
//        else tryDecode(record.get(fieldPosition))
//
//      def safeDecodeFieldValue(record: IndexedRecord): Any =
//        if (fieldPosition == -1) defaultFieldValue
//        else {
//          val schemaField = record.getSchema.getField(fieldName.get)
//          if (schemaField == null) defaultFieldValue else tryDecode(record.get(schemaField.pos))
//        }
//
//      @inline
//      private def defaultFieldValue: Any = param.default match {
//        case Some(default) => default
//        // there is no default, so the field must be an option
//        case None => decoder.decode(null)
//      }
//
//      @inline
//      private def tryDecode(value: Any): Any =
//        try {
//          decoder.decode(value)
//        } catch {
//          case NonFatal(ex) => param.default.getOrElse(throw ex)
//        }
//    }
//
//    // using the apply method here to create a ValueDecoder while keeping the types consistent and making sure to
//    // not accidentally capture non-serializable objects as class parameters that are only needed for creating the decoder.
//    def apply(idx: Int,
//              env: DefinitionEnvironment[Decoder],
//              update: SchemaUpdate,
//              record: Schema,
//              ctx: CaseClass[Decoder, T],
//              fieldMapper: FieldMapper): (Option[Field], ValueDecoder) = {
//
//      val annotations = new AnnotationExtractors(param.annotations)
//      val (decoder, field, index) = update match {
//        case FullSchemaUpdate(sf) =>
//          // in a full schema update, the schema is the leading information and we derive decoder modifications from it.
//          // so we extract the field and create a schema update from its schema and apply it to the decoder
//          // via resolveDecoder.
//
//          val (field, fieldUpdate, index) = if (annotations.transient) {
//            // transient annotations still win over schema overrides.
//            (None, NoUpdate, -1)
//          } else {
//            val field = extractField(param, sf)
//            (Some(field), FullSchemaUpdate(SchemaFor(field.schema(), sf.fieldMapper)), field.pos)
//          }
//
//          val decoder = param.typeclass.resolveDecoder(env, fieldUpdate)
//          (decoder, field, index)
//
//        case _ =>
//          // Otherwise, we look for annotations on the field (such as AvroFixed or AvroNamespace) and use those to
//          // compute modifications to apply to the decoder.
//          // The field schema is then derived from the decoder schema.
//          val decoder = param.typeclass.resolveDecoder(env, fieldUpdate(param, record, fieldMapper))
//          if (annotations.transient) (decoder, None, -1)
//          else {
//            val field = buildField(param, record, ctx, decoder.schema, fieldMapper)
//            // idx is the index position of the magnolia param (derived from the Scala case class)
//            (decoder, Some(field), idx)
//          }
//      }
//
//      field -> new ValueDecoder(decoder, field.map(_.name), index)
//    }
//  }
//
//  /**
//    * Compute schema updates coming from annotations on the given parameter to be passed down to the
//    * field encoder / decoder. These may be a change of the schema type to fixed or an override of the namespace.
//    */
//  private def fieldUpdate[Typeclass[_]](param: Param[Typeclass, _],
//                                        record: Schema,
//                                        fieldMapper: FieldMapper): SchemaUpdate = {
//    val extractor = new AnnotationExtractors(param.annotations)
//    (extractor.fixed, extractor.namespace) match {
//      case (Some(size), namespace) =>
//        val name = extractor.name.getOrElse(fieldMapper.to(param.label))
//        val ns = namespace.getOrElse(record.getNamespace)
//        FullSchemaUpdate(SchemaFor(SchemaBuilder.fixed(name).namespace(ns).size(size), fieldMapper))
//      case (_, Some(ns)) => NamespaceUpdate(ns)
//      case _             => NoUpdate
//    }
//  }
//
//  private def buildField[Typeclass[_]](param: Param[Typeclass, _],
//                                       record: Schema,
//                                       ctx: CaseClass[Typeclass, _],
//                                       schema: Schema,
//                                       fieldMapper: FieldMapper) = {
//    val doc = Records.valueTypeDoc(ctx, param)
//    val namespace = record.getNamespace
//    Records.buildSchemaField(param, schema, new AnnotationExtractors(param.annotations), namespace, fieldMapper, doc)
//  }
//

//}
