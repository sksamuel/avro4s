package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{Encoder, FieldMapper}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class Tuple2Encoder[A, B](a: Encoder[A], b: Encoder[B]) extends Encoder[Tuple2[A, B]] {
  override def encode(tuple: (A, B), schema: Schema): Any = {
    val fieldA: Schema.Field = schema.getFields.get(0)
    val fieldB: Schema.Field = schema.getFields.get(1)
    val record = GenericData.Record(null)
    record.put("_1", a.encode(tuple._1, fieldA.schema()))
    record.put("_2", b.encode(tuple._2, fieldB.schema()))
    record
  }
}

trait TupleEncoders:
  given[A, B](using a: Encoder[A], b: Encoder[B]): Encoder[Tuple2[A, B]] = Tuple2Encoder(a, b)