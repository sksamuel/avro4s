package com.sksamuel.avro4s

import org.apache.avro.Schema

/**
 * Indicates that this typeclass (Codec, Encoder, Decoder) has a schema and is modifiable through schema changes
 * @tparam Typeclass type extending SchemaAware (Codec, Encoder, Decoder)
 * @tparam T data type this codec/encoder/decoder transforms from/to
 */
trait SchemaAware[+Typeclass[_], T] {

  def schemaFor: SchemaFor[T]

  final def schema: Schema = schemaFor.schema

  /**
   * Creates a derivation of this type using customizations given by the schema
   */
  def withSchema(schemaFor: SchemaFor[T]): Typeclass[T]

}
