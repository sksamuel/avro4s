package com.sksamuel.avro4s

import org.apache.avro.{Schema, SchemaBuilder}

object BigDecimals {
  implicit object AsString extends SchemaFor[BigDecimal] {
    override def schema(fieldMapper: FieldMapper, context: SchemaFor.Context): Schema = SchemaBuilder.builder().stringType()
  }
}
