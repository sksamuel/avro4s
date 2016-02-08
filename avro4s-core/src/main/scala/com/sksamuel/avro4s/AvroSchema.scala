package com.sksamuel.avro4s

import org.apache.avro.Schema

import scala.language.experimental.macros

object AvroSchema {
  def apply[T <: Product](implicit toAvroSchema: ToSchema[T]): Schema = toAvroSchema()
}
