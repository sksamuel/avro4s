package com.sksamuel.avro4s.schemas

import scala.deriving.Mirror

trait RecordSchemas {
  inline given derived[T](using m: Mirror.Of[T]): SchemaFor[T] = Macros.derive[T]
}
