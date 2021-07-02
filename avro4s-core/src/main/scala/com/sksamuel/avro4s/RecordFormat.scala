package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

/**
  * Brings together [[ToRecord]] and [[FromRecord]] in a single interface.
  */
@deprecated("import com.sksamuel.avro4s.{asRecord, decode} and use decode[T](<>) and/or <>.asRecord instead", since = "5.0.0")
trait RecordFormat[T] extends ToRecord[T] with FromRecord[T] with Serializable

/**
  * Returns a [[RecordFormat]] that will convert to/from
  * instances of T and avro Record's.
  */
@deprecated("import com.sksamuel.avro4s.{asRecord, decode} and use decode[T](<>) and/or <>.asRecord instead", since = "5.0.0")
object RecordFormat {

  def apply[T](schema: Schema)(using toRecord: ToRecord[T], fromRecord: FromRecord[T]): RecordFormat[T] = new RecordFormat[T] {
    override def from(record: IndexedRecord): T = fromRecord.from(record)
    override def to(t: T): Record = toRecord.to(t)
  }
}