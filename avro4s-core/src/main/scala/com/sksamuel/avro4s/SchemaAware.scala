package com.sksamuel.avro4s

import org.apache.avro.Schema

trait SchemaAware[+Typeclass[_], T] {

  def schemaFor: SchemaForV2[T]

  final def schema: Schema = schemaFor.schema

  def withSchema(schemaFor: SchemaForV2[T]): Typeclass[T]

}
