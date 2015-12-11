package com.sksamuel.avro4s

import scala.language.experimental.macros

object AvroImplicits {
  implicit def schemaFor[T]: AvroSchema[T] = macro SchemaMacros.schemaImpl[T]
  implicit def populatorFor[T]: AvroPopulator[T] = macro Readers.impl[T]
}