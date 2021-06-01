package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.{DefaultFieldMapper, FieldMapper, SchemaConfiguration, SchemaFor}
import org.apache.avro.Schema

trait CollectionSchemas:

  given[T](using schemaFor: SchemaFor[T]): SchemaFor[Array[T]] = new SchemaFor[Array[T]] :
    override def schema(config: SchemaConfiguration): Schema = Schema.createArray(schemaFor.schema(config))

  given[T](using schemaFor: SchemaFor[T]): SchemaFor[Seq[T]] = new SchemaFor[Seq[T]] :
    override def schema(config: SchemaConfiguration): Schema = Schema.createArray(schemaFor.schema(config))