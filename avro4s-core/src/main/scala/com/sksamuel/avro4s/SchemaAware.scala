package com.sksamuel.avro4s

import org.apache.avro.Schema

trait SchemaAware[+Typeclass[_], T] {

  def schema: Schema

  def withSchema(schemaFor: SchemaForV2[T]): Typeclass[T]

}
