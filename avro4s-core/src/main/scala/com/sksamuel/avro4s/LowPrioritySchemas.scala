package com.sksamuel.avro4s

import com.sksamuel.avro4s.schemas.MacroSchemaFor
import scala.deriving.Mirror

trait LowPrioritySchemas {
  inline given derived[T](using m: Mirror.Of[T]) : SchemaFor[T] = MacroSchemaFor.derive[T]
}
