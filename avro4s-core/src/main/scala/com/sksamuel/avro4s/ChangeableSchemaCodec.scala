package com.sksamuel.avro4s

import org.apache.avro.Schema

trait ChangeableSchemaCodec[T] { self: Codec[T] =>

  def withSchema(schema: Schema): Codec[T]

}
