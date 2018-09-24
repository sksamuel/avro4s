package com.sksamuel.avro4s

import com.sksamuel.avro4s.internal.{AvroSchema, DataTypeFor, Encoder, Record}
import org.apache.avro.Schema

trait ToRecord[T] extends Serializable {
  def to(t: T): Record
}

object ToRecord {
  def apply[T: Encoder : DataTypeFor]: ToRecord[T] = apply(AvroSchema[T])
  def apply[T: Encoder](schema: Schema): ToRecord[T] = new ToRecord[T] {
    override def to(t: T): Record = implicitly[Encoder[T]].encode(t, schema).asInstanceOf[Record]
  }
}