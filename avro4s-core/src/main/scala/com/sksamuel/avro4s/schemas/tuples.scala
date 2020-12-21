package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.SchemaFor
import org.apache.avro.{Schema, SchemaBuilder}

trait TupleSchemas:

  given [A, B](using a: SchemaFor[A], b: SchemaFor[B]) : SchemaFor[Tuple2[A, B]] =
    val fields = java.util.ArrayList[Schema.Field]()
    fields.add(new Schema.Field("_1", a.schema))
    fields.add(new Schema.Field("_2", b.schema))
    SchemaFor(Schema.createRecord("Tuple2", null, "scala", false, fields))

  given [A, B, C](using a: SchemaFor[A], b: SchemaFor[B], c:SchemaFor[C]) : SchemaFor[Tuple3[A, B,C]] =
    val fields = java.util.ArrayList[Schema.Field]()
    fields.add(new Schema.Field("_1", a.schema))
    fields.add(new Schema.Field("_2", b.schema))
    fields.add(new Schema.Field("_3", c.schema))
    SchemaFor(Schema.createRecord("Tuple3", null, "scala", false, fields))

  given [A, B, C, D](using a: SchemaFor[A], b: SchemaFor[B], c: SchemaFor[C], d: SchemaFor[D]) : SchemaFor[Tuple4[A, B, C, D]] =
    val fields = java.util.ArrayList[Schema.Field]()
    fields.add(new Schema.Field("_1", a.schema))
    fields.add(new Schema.Field("_2", b.schema))
    fields.add(new Schema.Field("_3", c.schema))
    fields.add(new Schema.Field("_4", d.schema))
    SchemaFor(Schema.createRecord("Tuple4", null, "scala", false, fields))

  given [A, B, C, D, E](using a: SchemaFor[A], b: SchemaFor[B], c: SchemaFor[C], d: SchemaFor[D], e: SchemaFor[E]) : SchemaFor[Tuple5[A, B, C, D, E]] =
    val fields = java.util.ArrayList[Schema.Field]()
    fields.add(new Schema.Field("_1", a.schema))
    fields.add(new Schema.Field("_2", b.schema))
    fields.add(new Schema.Field("_3", c.schema))
    fields.add(new Schema.Field("_4", d.schema))
    fields.add(new Schema.Field("_5", e.schema))
    SchemaFor(Schema.createRecord("Tuple5", null, "scala", false, fields))