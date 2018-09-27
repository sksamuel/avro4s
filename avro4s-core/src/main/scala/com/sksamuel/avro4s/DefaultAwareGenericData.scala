package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class DefaultAwareGenericData extends GenericData {
  override def newRecord(old: scala.Any, schema: Schema): AnyRef = {
    import scala.collection.JavaConverters._
    schema.getFields.asScala.foldLeft(new GenericData.Record(schema)) { case (record, field) =>
      record.put(field.name, field.defaultVal())
      record
    }
  }
}