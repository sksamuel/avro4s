package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

trait TupleEncoders:
  given [A, B](using a: Encoder[A], b: Encoder[B]): Encoder[Tuple2[A, B]] = new Encoder[Tuple2[A, B]] {
    override def encode(schema: Schema): ((A, B)) => Any = {
      
      val encoderA = a.encode(schema.getFields.get(0).schema())
      val encoderB = b.encode(schema.getFields.get(1).schema())
      
      val fieldA = schema.getFields.get(0).name()
      val fieldB = schema.getFields.get(1).name()
      
      { (a, b) =>
        val record = GenericData.Record(schema)
        record.put(fieldA, encoderA(a))
        record.put(fieldA, encoderB(b))
        record
      }
    }
  }