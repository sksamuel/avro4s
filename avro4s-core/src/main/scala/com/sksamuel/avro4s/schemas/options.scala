package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.{SchemaConfiguration, SchemaFor}
import org.apache.avro.{Schema, SchemaBuilder}

trait OptionSchemas {

  given NoneSchemaFor: SchemaFor[None.type] = Options.noneSchemaFor

  given[T](using schemaFor: SchemaFor[T]): SchemaFor[Option[T]] = new SchemaFor[Option[T]] {
    override def schema(config: SchemaConfiguration): Schema = {
      Schema.createUnion(Schema.create(Schema.Type.NULL), schemaFor.schema(config))
    }
  }
}


object Options:

  val noneSchemaFor: SchemaFor[None.type] =
    SchemaFor(SchemaBuilder.builder.nullType)