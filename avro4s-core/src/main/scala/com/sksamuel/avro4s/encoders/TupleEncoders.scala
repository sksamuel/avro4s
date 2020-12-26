package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.FieldMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class Tuple2Encoder[A, B](a: Encoder[A], b: Encoder[B]) extends Encoder[Tuple2[A, B]] {
  override def encode(schema: Schema, mapper: FieldMapper): ((A, B)) => Any = {
    val fieldA = schema.getFields.get(0)
    val fieldB = schema.getFields.get(1)
    { tuple =>
      val record = GenericData.Record(null)
      record.put("_1", a.encode(fieldA.schema(), mapper).apply(tuple._1))
      record.put("_2", b.encode(fieldB.schema(), mapper).apply(tuple._2))
      record
    }
  }
}

trait TupleEncoders:
  given [A, B](using a: Encoder[A], b: Encoder[B]): Encoder[Tuple2[A, B]] = Tuple2Encoder(a,b) 