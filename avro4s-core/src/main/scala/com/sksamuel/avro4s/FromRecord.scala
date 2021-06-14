package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

/**
  * Converts from an Avro [[IndexedRecord]] into instances of T.
  */
trait FromRecord[T] extends Serializable {
  def from(record: IndexedRecord): T
}

object FromRecord {
  def apply[T](schema: Schema)(using decoder: Decoder[T]): FromRecord[T] = new FromRecord[T] {
    override def from(record: IndexedRecord): T = decoder.decode(schema).apply(record)
  }
}