package com.sksamuel.avro4s.internal

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

/**
  * An implementation of [[org.apache.avro.generic.GenericContainer]] that is both a
  * [[GenericRecord]] and a [[SpecificRecord]].
  */
trait Record extends GenericRecord with SpecificRecord

case class ImmutableRecord(schema: Schema, values: Vector[AnyRef]) extends Record {

  import scala.collection.JavaConverters._

  override def put(key: String, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")
  override def put(i: Int, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")

  override def get(key: String): AnyRef = {
    val index = schema.getFields.asScala.indexWhere(_.name == key)
    get(index)
  }

  override def get(i: Int): AnyRef = values(i)
  override def getSchema: Schema = schema
}
