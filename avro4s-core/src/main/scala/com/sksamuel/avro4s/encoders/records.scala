package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.typeutils.{Annotations, Names}
import com.sksamuel.avro4s.{Avro4sConfigurationException, Encoder, ImmutableRecord, SchemaFor}
import magnolia1.CaseClass
import org.apache.avro.Schema

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

class RecordEncoder[T](ctx: magnolia1.CaseClass[Encoder, T]) extends Encoder[T] {

  def encode(schema: Schema): T => Any = {
    // the order of the encoders comes from the schema
    val encoders: Array[FieldEncoder[T]] = schema.getFields.asScala.map { field =>
      val param = findParam(field, ctx)
      if (param.isEmpty)
        throw new Avro4sConfigurationException(s"Unable to find case class parameter for field ${field.name()}")
      new FieldEncoder(param.get, field.schema())
    }.toArray
    { t => encodeT(schema, encoders, t) }
  }

  /**
    * Finds the matching param from the case class for the given avro [[Schema.Field]].
    */
  private def findParam(field: Schema.Field, ctx: magnolia1.CaseClass[Encoder, T]): Option[CaseClass.Param[Encoder, T]] = {
    ctx.params.find { param =>
      val annotations = new Annotations(param.annotations)
      val paramName = annotations.name.getOrElse(param.label)
      paramName == field.name()
    }
  }

  private def encodeT(schema: Schema, encoders: Array[FieldEncoder[T]], t: T): ImmutableRecord = {
    // hot code path. Sacrificing functional programming to the gods of performance.
    val length = encoders.length
    val values = new Array[Any](length)
    var i = 0
    while (i < length) {
      values(i) = encoders(i).encode(t)
      i += 1
    }
    ImmutableRecord(schema, values.toSeq)
  }
}

class FieldEncoder[T](param: magnolia1.CaseClass.Param[Encoder, T], schema: Schema) extends Serializable :

  private val encode = param.typeclass.encode(schema)

  def encode(record: T): Any = {
    val value = param.deref(record)
    encode.apply(value)
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