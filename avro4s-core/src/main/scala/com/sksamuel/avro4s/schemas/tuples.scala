package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.SchemaFor
import org.apache.avro.{Schema, SchemaBuilder}

trait TupleSchemas:

  given [A, B](using a: SchemaFor[A], b: SchemaFor[B]) : SchemaFor[Tuple2[A, B]] =
    val fields = java.util.ArrayList[Schema.Field]()
    fields.add(new Schema.Field("_1", a.schema))
    fields.add(new Schema.Field("_2", b.schema))
    SchemaFor(Schema.createRecord("Tuple2", null, "scala", false, fields))