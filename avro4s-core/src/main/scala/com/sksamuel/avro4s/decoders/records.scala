package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.encoders.FieldEncoder
import com.sksamuel.avro4s.typeutils.Annotations
import com.sksamuel.avro4s.{Avro4sDecodingException, Decoder, Encoder, ImmutableRecord}
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

class RecordDecoder[T](ctx: magnolia.CaseClass[Decoder, T]) extends Decoder[T] {

  override def decode(schema: Schema): Any => T = {
    val decoders: Array[FieldDecoder[T]] = ctx.params
      .map { param =>
        val annos = Annotations(param.annotations)
        if (annos.transient) TransientFieldDecoder else new SchemaFieldDecoder(param, schema)
      }.toArray
    { t => decodeT(schema, decoders, t) }
  }

  private def decodeT(schema: Schema, decoders: Array[FieldDecoder[T]], value: Any): T = value match {
    case record: IndexedRecord =>
      // hot code path. Sacrificing functional programming to the gods of performance.
      val length = decoders.length
      val values = new Array[Any](length)
      var i = 0
      while (i < length) {
        values(i) = decoders(i).decode(record)
        i += 1
      }
      ctx.rawConstruct(values.toIndexedSeq)
    case _ =>
      throw new Avro4sDecodingException(
        s"This decoder can only handle IndexedRecords or its subtypes such as GenericRecord [was ${value.getClass}]",
        value)
  }
}

trait FieldDecoder[+T] extends Serializable {
  def decode(record: IndexedRecord): Any
}

/**
  * Fields marked with @AvroTransient are always decoded as None's.
  */
object TransientFieldDecoder extends FieldDecoder[Nothing] {
  override def decode(record: IndexedRecord): Any = None
}

/**
  * Decodes normal fields based on the schema.
  */
class SchemaFieldDecoder[T](param: magnolia.CaseClass.Param[Decoder, T], schema: Schema) extends FieldDecoder[T] :
  require(schema.getType == Schema.Type.RECORD)

  private val fieldName = Annotations(param.annotations).name.getOrElse(param.label)
  private val fieldPosition = schema.getFields.asScala.indexWhere(_.name() == fieldName)
  private val field = schema.getField(fieldName)
  private val decoder = param.typeclass.asInstanceOf[Decoder[T]].decode(field.schema())
  private var fast: Boolean | Null = _

  override def decode(record: IndexedRecord): Any = {
    // testing schema based on reference equality, as value equality on schemas is probably more expensive to check
    // than using safe decoding.
    if (fast == null) fast = (record.getSchema() eq schema)
    if (fast == true) {
      // known schema, use fast pre-computed position-based decoding
      fastDecodeFieldValue(record)
    } else {
      // use safe name-based decoding
      safeDecodeFieldValue(record)
    }
  }

  def fastDecodeFieldValue(record: IndexedRecord): Any =
    if (fieldPosition == -1) defaultFieldValue
    else tryDecode(record.get(fieldPosition))

  def safeDecodeFieldValue(record: IndexedRecord): Any =
    if (fieldPosition == -1) defaultFieldValue
    else {
      val schemaField = record.getSchema.getField(fieldName)
      if (schemaField == null) defaultFieldValue else tryDecode(record.get(schemaField.pos))
    }

  @inline
  def defaultFieldValue: Any = param.default match {
    case Some(default) => default
    // there is no default, so the field must be an option
    case None => decoder.apply(null)
  }

  @inline
  def tryDecode(value: Any): Any =
    try {
      decoder.apply(value)
    } catch {
      case NonFatal(ex) => param.default.getOrElse(throw ex)
    }