//package com.sksamuel.avro4s
//
//import org.apache.avro.generic.GenericRecord
//
//trait RecordFormat[T] {
//  def to(t: T): GenericRecord
//  def from(record: GenericRecord): T
//}
//
//object RecordFormat {
//  implicit def apply[T](implicit toRecord: ToRecord[T], fromRecord: FromRecord[T]): RecordFormat[T] = new RecordFormat[T] {
//    override def from(record: GenericRecord): T = fromRecord(record)
//    override def to(t: T): GenericRecord = toRecord(t)
//  }
//}