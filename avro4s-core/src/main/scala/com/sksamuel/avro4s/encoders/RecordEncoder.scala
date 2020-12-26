package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.FieldMapper
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.jdk.CollectionConverters._

class RecordEncoder[T](mapper: FieldMapper, extractors: Map[String, Extractor[T]]) extends Encoder[T] {
  override def encode(schema: Schema, mapper: FieldMapper): T => Any = { t =>
    val record: GenericRecord = new GenericData.Record(schema)
    schema.getFields.asScala.foreach { field =>
      //record.put(field.name(), extractors.get(field.name()).get.getValue(t))
    }
    record
  }
}

trait Extractor[T] {
  def getValue(t: T): Any
}