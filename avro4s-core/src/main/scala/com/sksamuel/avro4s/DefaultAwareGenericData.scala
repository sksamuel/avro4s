package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class DefaultAwareGenericData extends GenericData {
  override def newRecord(old: scala.Any, schema: Schema): AnyRef = {
    import scala.collection.JavaConverters._
    val record = new GenericData.Record(schema)
    schema.getFields.asScala.foreach { field =>
      record.put(field.name, field.defaultVal)
    }
    record
  }
}