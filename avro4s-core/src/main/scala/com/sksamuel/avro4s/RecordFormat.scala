package com.sksamuel.avro4s

import com.sksamuel.avro4s.internal.{AvroSchema, DataTypeFor, Encoder, Record}
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

trait RecordFormat[T] extends Serializable {
  def to(t: T): Record
  def from(record: IndexedRecord): T
}

/**
  * Returns a [[RecordFormat]] that will convert to/from
  * instances of T and avro [[Record]]s.
  */
object RecordFormat {

  implicit def apply[T](implicit encoder: Encoder[T], dataTypeFor: DataTypeFor[T]): RecordFormat[T] = new RecordFormat[T] {
    private val schema = AvroSchema[T]
    override def from(record: IndexedRecord): T = ???
    override def to(t: T): Record = encoder.encode(t, schema).asInstanceOf[Record]
  }

  implicit def apply[T](schema: Schema)(implicit encoder: Encoder[T]): RecordFormat[T] = new RecordFormat[T] {
    override def from(record: IndexedRecord): T = ???
    override def to(t: T): Record = encoder.encode(t, schema).asInstanceOf[Record]
  }
}