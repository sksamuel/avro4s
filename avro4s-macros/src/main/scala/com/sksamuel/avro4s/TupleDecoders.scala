package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

trait TupleDecoders {

  implicit def tuple2Decoder[A, B](implicit decoderA: Decoder[A], decoderB: Decoder[B]) = new Decoder[(A, B)] {
    override def decode(t: Any, schema: Schema): (A, B) = {
      val record = t.asInstanceOf[GenericRecord]
      (decoderA.decode(record.get("_1"), schema), decoderB.decode(record.get("_2"), schema))
    }
  }
}
