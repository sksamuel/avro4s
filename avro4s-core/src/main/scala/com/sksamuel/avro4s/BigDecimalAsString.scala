package com.sksamuel.avro4s

import org.apache.avro.{Schema, SchemaBuilder}

object BigDecimalAsString {
  implicit object BigDecimalAsStringSchemaFor extends SchemaFor[BigDecimal] {
    override def schema: Schema = SchemaBuilder.builder().stringType()
  }
}