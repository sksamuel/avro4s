package com.sksamuel.avro4s

import org.apache.avro.SchemaBuilder

object BigDecimalAsString {
  implicit object BigDecimalAsStringSchemaFor extends SchemaFor[BigDecimal] {
    override def schema(implicit namingStrategy: NamingStrategy) = SchemaBuilder.builder().stringType()
  }
}