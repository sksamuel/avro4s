package com.sksamuel.avro4s

import org.apache.avro.Schema

trait TupleEncoders {

  implicit def tuple2Encoder[A, B](implicit encA: Encoder[A], encB: Encoder[B]) = new Encoder[(A, B)] {
    override def encode(t: (A, B), schema: Schema): AnyRef = {
      ImmutableRecord(
        schema,
        Vector(
          encA.encode(t._1, schema.getField("_1").schema()),
          encB.encode(t._2, schema.getField("_2").schema()))
      )
    }
  }

  implicit def tuple3Encoder[A, B, C](implicit encA: Encoder[A], encB: Encoder[B], encC: Encoder[C]) = new Encoder[(A, B, C)] {
    override def encode(t: (A, B, C), schema: Schema): AnyRef = {
      ImmutableRecord(
        schema,
        Vector(
          encA.encode(t._1, schema.getField("_1").schema()),
          encB.encode(t._2, schema.getField("_2").schema()),
          encC.encode(t._3, schema.getField("_3").schema()))
      )
    }
  }

  implicit def tuple4Encoder[A, B, C, D](implicit encA: Encoder[A], encB: Encoder[B], encC: Encoder[C], encD: Encoder[D]) = new Encoder[(A, B, C, D)] {
    override def encode(t: (A, B, C, D), schema: Schema): AnyRef = {
      ImmutableRecord(
        schema,
        Vector(
          encA.encode(t._1, schema.getField("_1").schema()),
          encB.encode(t._2, schema.getField("_2").schema()),
          encC.encode(t._3, schema.getField("_3").schema()),
          encD.encode(t._4, schema.getField("_4").schema()))
      )
    }
  }

  implicit def tuple5Encoder[A, B, C, D, E](implicit encA: Encoder[A], encB: Encoder[B], encC: Encoder[C], encD: Encoder[D], encE: Encoder[E]) = new Encoder[(A, B, C, D, E)] {
    override def encode(t: (A, B, C, D, E), schema: Schema): AnyRef = {
      ImmutableRecord(
        schema,
        Vector(
          encA.encode(t._1, schema.getField("_1").schema()),
          encB.encode(t._2, schema.getField("_2").schema()),
          encC.encode(t._3, schema.getField("_3").schema()),
          encD.encode(t._4, schema.getField("_4").schema()),
          encE.encode(t._5, schema.getField("_5").schema()))
      )
    }
  }
}
