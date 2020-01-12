package com.sksamuel.avro4s

import org.apache.avro.Schema

trait FieldSpecificCodec[T] { self: Codec[T] =>

  def forFieldWith(schema: Schema, annotations: Seq[Any]): Codec[T]

}
