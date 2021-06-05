package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{Encoder, FieldMapper}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class Tuple2Encoder[A, B](a: Encoder[A], b: Encoder[B]) extends Encoder[Tuple2[A, B]] {
  override def encode(schema: Schema): ((A, B)) => Any = {
    val fieldA: Schema.Field = schema.getFields.get(0)
    val fieldB: Schema.Field = schema.getFields.get(1)
    val encoderA = a.encode(fieldA.schema())
    val encoderB = b.encode(fieldB.schema())
    { tuple =>
      val record = GenericData.Record(schema)
      record.put("_1", encoderA.apply(tuple._1))
      record.put("_2", encoderB.apply(tuple._2))
      record
    }
  }
}

trait TupleEncoders:
  given[A, B](using a: Encoder[A], b: Encoder[B]): Encoder[Tuple2[A, B]] = Tuple2Encoder(a, b)