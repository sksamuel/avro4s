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

  def apply[T](schema: Schema)(using toRecord: ToRecord[T], fromRecord: FromRecord[T]): RecordFormat[T] = new RecordFormat[T] {
    override def from(record: IndexedRecord): T = fromRecord.from(record)
    override def to(t: T): Record = toRecord.to(t)
  }

  def fromCodec[T: Codec](schema: Schema) = new RecordFormat[T] {
    val toRecord: ToRecord[T] = ToRecord.apply[T](schema)
    val fromRecord: FromRecord[T] = FromRecord.apply[T](schema)

    override def from(record: IndexedRecord): T = fromRecord.from(record)
    override def to(t: T): Record = toRecord.to(t)
  }
}