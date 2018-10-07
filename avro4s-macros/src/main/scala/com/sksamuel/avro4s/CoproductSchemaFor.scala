package com.sksamuel.avro4s

import org.apache.avro.Schema
import shapeless.{:+:, CNil, Coproduct, Generic}

trait CoproductSchemaFor {
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
