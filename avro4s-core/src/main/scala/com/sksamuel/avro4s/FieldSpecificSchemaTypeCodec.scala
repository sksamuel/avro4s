package com.sksamuel.avro4s

import org.apache.avro.Schema

trait FieldSpecificSchemaTypeCodec[T] { self: Codec[T] =>

  def withFieldSchema(schema: Schema): Codec[T]

}
