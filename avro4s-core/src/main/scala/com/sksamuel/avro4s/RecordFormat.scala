//package com.sksamuel.avro4s
//
//import org.apache.avro.generic.IndexedRecord
//
///**
//  * Brings together [[ToRecord]] and [[FromRecord]] in a single interface.
//  */
//trait RecordFormat[T] extends ToRecord[T] with FromRecord[T] with Serializable
//
///**
//  * Returns a [[RecordFormat]] that will convert to/from
//  * instances of T and avro Record's.
//  */
//object RecordFormat {
//
//  def apply[T: Encoder : Decoder]: RecordFormat[T] = new RecordFormat[T] {
//    private val fromRecord = FromRecord[T]
//    private val toRecord = ToRecord[T]
//
//    override def from(record: IndexedRecord): T = fromRecord.from(record)
//    override def to(t: T): Record = toRecord.to(t)
//  }
//}