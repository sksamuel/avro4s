package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.{SchemaConfiguration, SchemaFor}
import org.apache.avro.Schema

trait OptionSchemas {
  given [T](using schemaFor: SchemaFor[T]) : SchemaFor[Option[T]] = new SchemaFor[Option[T]] {
    override def schema(config: SchemaConfiguration): Schema = {
      Schema.createUnion(Schema.create(Schema.Type.NULL), schemaFor.schema(config))
    }
  }
}
