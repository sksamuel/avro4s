package com.sksamuel.avro4s

import org.apache.avro.{Schema, SchemaBuilder}

trait TupleSchemaFor {

  implicit def tuple2SchemaFor[A, B](implicit a: SchemaFor[A], b: SchemaFor[B]): SchemaFor[(A, B)] = new SchemaFor[(A, B)] {
    override def schema: Schema =
      SchemaBuilder.record("Tuple2").namespace("scala").doc(null)
        .fields()
        .name("_1").`type`(a.schema).noDefault()
        .name("_2").`type`(b.schema).noDefault()
        .endRecord()
  }

  implicit def tuple3SchemaFor[A, B, C](implicit
                                        a: SchemaFor[A],
                                        b: SchemaFor[B],
                                        c: SchemaFor[C]): SchemaFor[(A, B, C)] = new SchemaFor[(A, B, C)] {
    override def schema: Schema =
      SchemaBuilder.record("Tuple3").namespace("scala").doc(null)
        .fields()
        .name("_1").`type`(a.schema).noDefault()
        .name("_2").`type`(b.schema).noDefault()
        .name("_3").`type`(c.schema).noDefault()
        .endRecord()
  }

  implicit def tuple4SchemaFor[A, B, C, D](implicit
                                           a: SchemaFor[A],
                                           b: SchemaFor[B],
                                           c: SchemaFor[C],
                                           d: SchemaFor[D]): SchemaFor[(A, B, C, D)] = new SchemaFor[(A, B, C, D)] {
    override def schema: Schema =
      SchemaBuilder.record("Tuple4").namespace("scala").doc(null)
        .fields()
        .name("_1").`type`(a.schema).noDefault()
        .name("_2").`type`(b.schema).noDefault()
        .name("_3").`type`(c.schema).noDefault()
        .name("_4").`type`(d.schema).noDefault()
        .endRecord()
  }

  implicit def tuple5SchemaFor[A, B, C, D, E](implicit
                                              a: SchemaFor[A],
                                              b: SchemaFor[B],
                                              c: SchemaFor[C],
                                              d: SchemaFor[D],
                                              e: SchemaFor[E]): SchemaFor[(A, B, C, D, E)] = new SchemaFor[(A, B, C, D, E)] {
    override def schema: Schema =
      SchemaBuilder.record("Tuple5").namespace("scala").doc(null)
        .fields()
        .name("_1").`type`(a.schema).noDefault()
        .name("_2").`type`(b.schema).noDefault()
        .name("_3").`type`(c.schema).noDefault()
        .name("_4").`type`(d.schema).noDefault()
        .name("_5").`type`(e.schema).noDefault()
        .endRecord()
  }


}
