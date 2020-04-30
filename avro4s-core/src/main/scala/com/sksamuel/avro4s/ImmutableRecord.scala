package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

/**
  * An implementation of org.apache.avro.generic.GenericContainer that is both a
  * GenericRecord and a SpecificRecord.
  */
trait Record extends GenericRecord with SpecificRecord

case class ImmutableRecord(schema: Schema, values: Seq[AnyRef]) extends Record {
  import scala.collection.JavaConverters._

  require(schema.getType == Schema.Type.RECORD, "Cannot create an ImmutableRecord with a schema that is not a RECORD")
  require(schema.getFields.size == values.size,
    s"Schema field size (${schema.getFields.size}) and value array size (${values.size}) must match")
  require(schema.getFields.asScala.forall(_.pos < values.size),
    s"Schema field position(s) (${schema.getFields.asScala.map(_.pos).filter(_ >= values.size).mkString(", ")}) must be within value array size (${values.size})")

  private val indices: Map[String, Int] = schema.getFields.asScala.map(f => f.name -> f.pos).toMap

  override def put(key: String, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")
  override def put(i: Int, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")

  override def get(key: String): AnyRef = {
    val index = indices.getOrElse(key, sys.error(s"Field $key does not exist in this record (schema=$schema, values=$values)"))
    get(index)
  }

  override def get(i: Int): AnyRef = values(i)
  override def getSchema: Schema = schema
}
