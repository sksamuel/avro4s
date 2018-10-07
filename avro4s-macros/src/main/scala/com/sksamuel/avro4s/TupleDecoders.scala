package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

trait TupleDecoders {

  implicit def tuple2Decoder[A, B](implicit
                                   decoderA: Decoder[A],
                                   decoderB: Decoder[B]
                                  ) = new Decoder[(A, B)] {
    override def decode(t: Any, schema: Schema): (A, B) = {
      val record = t.asInstanceOf[GenericRecord]
      (
        decoderA.decode(record.get("_1"), schema),
        decoderB.decode(record.get("_2"), schema)
      )
    }
  }

  implicit def tuple3Decoder[A, B, C, D, E](implicit
                                            decoderA: Decoder[A],
                                            decoderB: Decoder[B],
                                            decoderC: Decoder[C]
                                           ) = new Decoder[(A, B, C)] {
    override def decode(t: Any, schema: Schema): (A, B, C) = {
      val record = t.asInstanceOf[GenericRecord]
      (
        decoderA.decode(record.get("_1"), schema),
        decoderB.decode(record.get("_2"), schema),
        decoderC.decode(record.get("_3"), schema)
      )
    }
  }

  implicit def tuple4Decoder[A, B, C, D, E](implicit
                                            decoderA: Decoder[A],
                                            decoderB: Decoder[B],
                                            decoderC: Decoder[C],
                                            decoderD: Decoder[D]
                                           ) = new Decoder[(A, B, C, D)] {
    override def decode(t: Any, schema: Schema): (A, B, C, D) = {
      val record = t.asInstanceOf[GenericRecord]
      (
        decoderA.decode(record.get("_1"), schema),
        decoderB.decode(record.get("_2"), schema),
        decoderC.decode(record.get("_3"), schema),
        decoderD.decode(record.get("_4"), schema)
      )
    }
  }

  implicit def tuple5Decoder[A, B, C, D, E](implicit
                                            decoderA: Decoder[A],
                                            decoderB: Decoder[B],
                                            decoderC: Decoder[C],
                                            decoderD: Decoder[D],
                                            decoderE: Decoder[E]
                                           ) = new Decoder[(A, B, C, D, E)] {
    override def decode(t: Any, schema: Schema): (A, B, C, D, E) = {
      val record = t.asInstanceOf[GenericRecord]
      (
        decoderA.decode(record.get("_1"), schema),
        decoderB.decode(record.get("_2"), schema),
        decoderC.decode(record.get("_3"), schema),
        decoderD.decode(record.get("_4"), schema),
        decoderE.decode(record.get("_5"), schema)
      )
    }
  }
}
