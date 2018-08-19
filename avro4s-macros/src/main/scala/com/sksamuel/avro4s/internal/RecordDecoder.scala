package com.sksamuel.avro4s.internal

import org.apache.avro.generic.IndexedRecord

// to be renamed back to [FromRecord]
trait RecordDecoder[T] {
  def apply(record: IndexedRecord): T
}
