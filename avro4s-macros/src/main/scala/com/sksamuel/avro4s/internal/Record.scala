package com.sksamuel.avro4s.internal

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

case class Record(schema: Schema, values: Array[AnyRef]) extends GenericRecord with SpecificRecord {

  //  override def put(key: String, value: AnyRef): Unit = {
  //    val field = schema.getField(key)
  //    if (field == null) throw new AvroRuntimeException("Not a valid schema field: " + key)
  //    values.update(field.pos, value)
  //  }

  override def get(i: Int): AnyRef = values(i)
  override def get(key: String): AnyRef = {
    val field = schema.getField(key)
    if (field == null) null else get(field.pos)
  }

  override def put(i: Int, v: AnyRef): Unit = ??? // values.update(i, v)
  override def put(key: String, v: scala.Any): Unit = ???

  override def getSchema: Schema = schema
}
