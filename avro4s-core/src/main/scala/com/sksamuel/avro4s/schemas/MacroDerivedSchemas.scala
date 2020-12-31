package com.sksamuel.avro4s.schemas

import scala.deriving.Mirror

trait MacroDerivedSchemas {
  inline given derived[T](using m: Mirror.Of[T]): SchemaFor[T] = Macros.derive[T]
}
