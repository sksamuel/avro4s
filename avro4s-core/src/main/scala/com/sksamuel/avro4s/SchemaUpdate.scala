package com.sksamuel.avro4s

sealed trait SchemaUpdate

object SchemaUpdate {
  case class FullSchemaUpdate(schemaFor: SchemaFor[_]) extends SchemaUpdate
  case class NamespaceUpdate(namespace: String) extends SchemaUpdate
  case object NoUpdate extends SchemaUpdate
}
