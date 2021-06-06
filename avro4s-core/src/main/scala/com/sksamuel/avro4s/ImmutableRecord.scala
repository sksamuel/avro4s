package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

/**
  * An implementation of org.apache.avro.generic.GenericContainer that is both a
  * GenericRecord and a SpecificRecord.
  */
trait Record extends GenericRecord with SpecificRecord

case class ImmutableRecord(schema: Schema, values: Seq[Any]) extends Record {

  require(schema.getType == Schema.Type.RECORD, "Cannot create an ImmutableRecord with a schema that is not a RECORD")
  require(schema.getFields.size == values.size,
    s"Schema field size (${schema.getFields.size}) and value Seq size (${values.size}) must match")

  override def put(key: String, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")
  override def put(i: Int, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")

  override def get(key: String): Any = {
    val field = schema.getField(key)
    if (field == null) sys.error(s"Field $key does not exist in this record (schema=$schema, values=$values)")
    values(field.pos)
  }

  override def get(i: Int): Any = values(i)
  override def getSchema: Schema = schema
}
