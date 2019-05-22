package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

/**
  * Brings together [[ToRecord]] and [[FromRecord]] in a single interface.
  */
trait RecordFormat[T] extends ToRecord[T] with FromRecord[T] with Serializable

/**
  * Returns a [[RecordFormat]] that will convert to/from
  * instances of T and avro Record's.
  */
object RecordFormat {

  def apply[T: Encoder : Decoder : SchemaFor]: RecordFormat[T] = apply(AvroSchema[T], DefaultNamingStrategy)

  def apply[T: Encoder : Decoder](schema: Schema, naming: NamingStrategy): RecordFormat[T] = new RecordFormat[T] {
    private val fromRecord = FromRecord[T](schema, naming)
    private val toRecord = ToRecord[T](schema)
    override def from(record: IndexedRecord): T = fromRecord.from(record)
    override def to(t: T): Record = toRecord.to(t)
  }
}