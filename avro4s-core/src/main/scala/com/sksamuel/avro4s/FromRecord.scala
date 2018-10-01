package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

/**
  * Converts from an Avro GenericRecord into instances of T.
  */
trait FromRecord[T <: Product] extends Serializable {
  def from(record: GenericRecord): T
}

object FromRecord {
  def apply[T <: Product : Decoder : SchemaFor]: FromRecord[T] = apply(AvroSchema[T])
  def apply[T <: Product : Decoder](schema: Schema): FromRecord[T] = new FromRecord[T] {
    override def from(record: GenericRecord): T = implicitly[Decoder[T]].decode(record)
  }
}