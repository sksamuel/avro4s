package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

trait RecordFormat[T] extends ToRecord[T] with FromRecord[T] with Serializable

/**
  * Returns a [[RecordFormat]] that will convert to/from
  * instances of T and avro Record's.
  */
object RecordFormat {

  def apply[T: Encoder : Decoder : SchemaFor]: RecordFormat[T] = apply(AvroSchema[T])

  def apply[T: Encoder : Decoder](schema: Schema): RecordFormat[T] = new RecordFormat[T] {
    private val fromRecord = FromRecord[T](schema)
    private val toRecord = ToRecord[T](schema)
    override def from(record: GenericRecord): T = fromRecord.from(record)
    override def to(t: T): Record = toRecord.to(t)
  }
}