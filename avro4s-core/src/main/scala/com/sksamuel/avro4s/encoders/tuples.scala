package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{Encoder, FieldMapper}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

trait TupleEncoders:
  given[A, B](using a: Encoder[A], b: Encoder[B]): Encoder[Tuple2[A, B]] = Tuple2Encoder(a, b)
  given[A, B, C](using a: Encoder[A], b: Encoder[B], c: Encoder[C]): Encoder[Tuple3[A, B, C]] = Tuple3Encoder(a, b, c)
  given[A, B, C, D](using a: Encoder[A], b: Encoder[B], c: Encoder[C], d: Encoder[D]): Encoder[Tuple4[A, B, C, D]] =
    Tuple4Encoder(a, b, c, d)
  given[A, B, C, D, E](using a: Encoder[A], b: Encoder[B], c: Encoder[C], d: Encoder[D], e: Encoder[E]): Encoder[Tuple5[A, B, C, D, E]] =
    Tuple5Encoder(a, b, c, d, e)

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

class Tuple3Encoder[A, B, C](a: Encoder[A], b: Encoder[B], c: Encoder[C]) extends Encoder[Tuple3[A, B, C]] {
  override def encode(schema: Schema): ((A, B, C)) => Any = {
    val fieldA: Schema.Field = schema.getFields.get(0)
    val fieldB: Schema.Field = schema.getFields.get(1)
    val fieldC: Schema.Field = schema.getFields.get(2)
    val encodeA = a.encode(fieldA.schema())
    val encodeB = b.encode(fieldB.schema())
    val encodeC = c.encode(fieldC.schema())
    { tuple =>
      val record = GenericData.Record(schema)
      record.put("_1", encodeA.apply(tuple._1))
      record.put("_2", encodeB.apply(tuple._2))
      record.put("_3", encodeC.apply(tuple._3))
      record
    }
  }
}

class Tuple4Encoder[A, B, C, D](a: Encoder[A], b: Encoder[B], c: Encoder[C], d: Encoder[D]) extends Encoder[Tuple4[A, B, C, D]] {
  override def encode(schema: Schema): ((A, B, C, D)) => Any = {
    val fieldA: Schema.Field = schema.getFields.get(0)
    val fieldB: Schema.Field = schema.getFields.get(1)
    val fieldC: Schema.Field = schema.getFields.get(2)
    val fieldD: Schema.Field = schema.getFields.get(3)
    val encodeA = a.encode(fieldA.schema())
    val encodeB = b.encode(fieldB.schema())
    val encodeC = c.encode(fieldC.schema())
    val encodeD = d.encode(fieldD.schema())
    { tuple =>
      val record = GenericData.Record(schema)
      record.put("_1", encodeA.apply(tuple._1))
      record.put("_2", encodeB.apply(tuple._2))
      record.put("_3", encodeC.apply(tuple._3))
      record.put("_4", encodeD.apply(tuple._4))
      record
    }
  }
}

class Tuple5Encoder[A, B, C, D, E](a: Encoder[A], b: Encoder[B], c: Encoder[C], d: Encoder[D], e: Encoder[E]) extends Encoder[Tuple5[A, B, C, D, E]] {
  override def encode(schema: Schema): ((A, B, C, D, E)) => Any = {
    val fieldA: Schema.Field = schema.getFields.get(0)
    val fieldB: Schema.Field = schema.getFields.get(1)
    val fieldC: Schema.Field = schema.getFields.get(2)
    val fieldD: Schema.Field = schema.getFields.get(3)
    val fieldE: Schema.Field = schema.getFields.get(4)
    val encodeA = a.encode(fieldA.schema())
    val encodeB = b.encode(fieldB.schema())
    val encodeC = c.encode(fieldC.schema())
    val encodeD = d.encode(fieldD.schema())
    val encodeE = e.encode(fieldE.schema())
    { tuple =>
      val record = GenericData.Record(schema)
      record.put("_1", encodeA.apply(tuple._1))
      record.put("_2", encodeB.apply(tuple._2))
      record.put("_3", encodeC.apply(tuple._3))
      record.put("_4", encodeD.apply(tuple._4))
      record.put("_5", encodeE.apply(tuple._5))
      record
    }
  }
}