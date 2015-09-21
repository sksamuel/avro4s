package com.sksamuel.avro4s

import scala.language.experimental.macros

object AvroImplicits {
  implicit def schemaFor[T]: AvroSchema[T] = macro Macros.schemaImpl[T]
  implicit def writerFor[T]: AvroSerializer[T] = macro Writers.impl[T]
  implicit def populatorFor[T]: AvroPopulator[T] = macro Readers.impl[T]
}