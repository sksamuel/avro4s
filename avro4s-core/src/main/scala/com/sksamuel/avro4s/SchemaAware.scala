package com.sksamuel.avro4s

import org.apache.avro.Schema

trait SchemaAware[+T[_], V] {

  def schema: Schema

  def withSchema(schemaFor: SchemaForV2[V], fieldMapper: FieldMapper = DefaultFieldMapper): T[V]

}
