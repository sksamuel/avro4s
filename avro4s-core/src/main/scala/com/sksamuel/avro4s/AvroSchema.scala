package com.sksamuel.avro4s

import org.apache.avro.Schema

object AvroSchema {

  /**
   * Generates an avro [[Schema]] for a type T using default configuration.
   */
  def apply[T](using schemaFor: SchemaFor[T]): Schema = apply(SchemaConfiguration.default)

  def apply[T](config: SchemaConfiguration)(using schemaFor: SchemaFor[T]): Schema = schemaFor.schema(config)
}
