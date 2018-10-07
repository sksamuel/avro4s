package com.sksamuel.avro4s

import org.apache.avro.{Schema, SchemaBuilder}
import shapeless.{:+:, CNil, Coproduct, Generic}

trait LowPrioritySchemaFor {

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

  // A coproduct is a union, or a generalised either.
  // A :+: B :+: C :+: CNil is a type that is either an A, or a B, or a C.

  // Shapeless's implementation builds up the type recursively,
  // (i.e., it's actually A :+: (B :+: (C :+: CNil)))
  // so here we define the schema for the base case of the recursion, C :+: CNil
  implicit def coproductBaseSchema[S](implicit basefor: SchemaFor[S]): SchemaFor[S :+: CNil] = new SchemaFor[S :+: CNil] {

    import scala.collection.JavaConverters._

    val base = basefor.schema
    val schemas = scala.util.Try(base.getTypes.asScala).getOrElse(Seq(base))
    override def schema = Schema.createUnion(schemas: _*)
  }

  // And here we continue the recursion up.
  implicit def coproductSchema[S, T <: Coproduct](implicit basefor: SchemaFor[S], coproductFor: SchemaFor[T]): SchemaFor[S :+: T] = new SchemaFor[S :+: T] {
    val base = basefor.schema
    val coproduct = coproductFor.schema
    override def schema: Schema = SchemaHelper.createSafeUnion(base, coproduct)
  }

  implicit def genCoproduct[T, C <: Coproduct](implicit gen: Generic.Aux[T, C],
                                               coproductFor: SchemaFor[C]): SchemaFor[T] = new SchemaFor[T] {
    override def schema: Schema = coproductFor.schema
  }
}
