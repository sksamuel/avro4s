package com.sksamuel.avro4s.internal

import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.{AvroRuntimeException, Schema}

class Record(schema: Schema) extends GenericRecord with SpecificRecord {

  private val values = Array.ofDim[AnyRef](schema.getFields.size)

  override def put(key: String, value: AnyRef): Unit = {
    val field = schema.getField(key)
    if (field == null) throw new AvroRuntimeException("Not a valid schema field: " + key)
    values.update(field.pos, value)
  }

  override def get(key: String): AnyRef = {
    val field = schema.getField(key)
    if (field == null) null else values(field.pos)
  }

  override def put(i: Int, v: AnyRef): Unit = values.update(i, v)

  override def get(i: Int): AnyRef = values(i)

  override def getSchema: Schema = schema
}
