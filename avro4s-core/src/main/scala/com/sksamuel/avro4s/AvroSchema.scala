package com.sksamuel.avro4s

import org.apache.avro.Schema

import scala.language.experimental.macros

object AvroSchema {
  def apply[T](implicit toAvroSchema: ToAvroSchema[T]): Schema = toAvroSchema()
}
