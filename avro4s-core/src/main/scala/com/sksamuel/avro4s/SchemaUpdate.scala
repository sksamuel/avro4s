package com.sksamuel.avro4s

sealed trait SchemaUpdate {
  def fieldMapper: FieldMapper
}

object SchemaUpdate {
  case class FullSchemaUpdate(schemaFor: SchemaFor[_]) extends SchemaUpdate {
    def fieldMapper: FieldMapper = schemaFor.fieldMapper
  }
  case class NamespaceUpdate(namespace: String, fieldMapper: FieldMapper) extends SchemaUpdate
  case class UseFieldMapper(fieldMapper: FieldMapper) extends SchemaUpdate
}
