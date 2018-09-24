package com.sksamuel.avro4s

import com.sksamuel.avro4s.internal.SchemaFor
import org.apache.avro.{Schema, SchemaBuilder}

object BigDecimalAsString {
  implicit object BigDecimalAsStringSchemaFor extends SchemaFor[BigDecimal] {
    override def schema: Schema = SchemaBuilder.builder().stringType()
  }
}