package com.sksamuel.avro4s

import org.apache.avro.Schema

trait SchemaAware[+Typeclass[_], T] {

  def schemaFor: SchemaFor[T]

  final def schema: Schema = schemaFor.schema

  def withSchema(schemaFor: SchemaFor[T]): Typeclass[T]

}
