package com.sksamuel.avro4s

import scala.language.experimental.macros

object SchemaGenerator {
  implicit def schemaFor[T]: AvroSchemaWriter[T] = macro Macros.classSchema_impl[T]
}
