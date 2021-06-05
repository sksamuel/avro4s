package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.typeutils.{Annotations, Names}
import com.sksamuel.avro4s.{Encoder, ImmutableRecord, SchemaFor}
import magnolia.CaseClass
import org.apache.avro.Schema

import scala.reflect.ClassTag

class RecordEncoder[T](ctx: magnolia.CaseClass[Encoder, T]) extends Encoder[T] {

  def encode(schema: Schema): T => Any = {
    val encoders = ctx.params
      .map { param => new FieldEncoder(param) }
      .map { it => it.encode(schema) }
      .toArray
    { t => encodeT(schema, encoders, t) }
  }

  private def encodeT(schema:Schema, encoders: Array[T => Any], t: T): ImmutableRecord = {
    // hot code path. Sacrificing functional programming to the gods of performance.
    val length = encoders.length
    val values = new Array[Any](length)
    var i = 0
    while (i < length) {
      values(i) = encoders(i).apply(t)
      i += 1
    }
    ImmutableRecord(schema, values) // note: array gets implicitly wrapped in an immutable container.
  }

  //  override def withSchema(schemaFor: SchemaFor[T]): Encoder[T] = {
  //    verifyNewSchema(schemaFor)
  //    encoder(ctx, new DefinitionEnvironment[Encoder](), FullSchemaUpdate(schemaFor), schemaFor.fieldMapper)
  //  }

  //  private def extractField[Typeclass[_]](param: Param[Typeclass, _], schemaFor: SchemaFor[_]): Field = {
  //    val annotations = new Annotations(param.annotations)
  //    val fieldName = annotations.name.getOrElse("") // (schemaFor.fieldMapper.to(param.label))
  //    val field = schemaFor.schema.getField(fieldName)
  //    if (field == null) {
  //      throw new Avro4sConfigurationException(
  //        s"Unable to find field with name $fieldName for case class parameter ${param.label} in schema ${schemaFor.schema}")
  //    }
  //    field
  //  }
}

class FieldEncoder[T](param: magnolia.CaseClass.Param[Encoder, T]) extends Encoder[T] with Serializable :

  private val name = Annotations(param.annotations).name.getOrElse(param.label)

  override def encode(schema: Schema): T => Any = {
    val fieldSchema = schema.getField(name).schema()
    val encoderF = param.typeclass.encode(fieldSchema)
    { t =>
      val value = param.deref(t)
      encoderF.apply(value)
    }
  }

//  // using inner class here to be able to reference param.PType below, and keep the type relation intact in
//  // the apply() method below.
//  class ValueEncoder(encoder: Encoder[param.PType], val fieldName: String) extends Serializable {
//    def encodeFieldValue(value: T): AnyRef = encoder.encode(param.dereference(value))
//  }

//  // using the apply method here to create a ValueEncoder while keeping the types consistent and making sure to
//  // not accidentally capture non-serializable objects as class parameters that are only needed for creating the encoder.
//  def apply(env: DefinitionEnvironment[Encoder],
//            update: SchemaUpdate,
//            record: Schema,
//            ctx: CaseClass[Encoder, T],
//            fieldMapper: FieldMapper): (Field, ValueEncoder) = {
//
//    val (encoder, field) = update match {
//      case FullSchemaUpdate(sf) =>
//        // in a full schema update, the schema is the leading information and we derive encoder modifications from it.
//        // so we extract the field and create a schema update from its schema and apply it to the encoder
//        // via resolveEncoder.
//        val field = extractField(param, sf)
//
//        val fieldUpdate = FullSchemaUpdate(SchemaFor(field.schema(), sf.fieldMapper))
//        val encoder = param.typeclass.resolveEncoder(env, fieldUpdate)
//        (encoder, field)
//
//      case _ =>
//        // Otherwise, we look for annotations on the field (such as AvroFixed or AvroNamespace) and use those to
//        // compute modifications to apply to the encoder.
//        // The field schema is then derived from the encoder schema.
//        val encoder = param.typeclass.resolveEncoder(env, fieldUpdate(param, record, fieldMapper))
//        (encoder, buildField(param, record, ctx, encoder.schema, fieldMapper))
//    }
//
//    field -> new ValueEncoder(encoder, field.name)
//  }