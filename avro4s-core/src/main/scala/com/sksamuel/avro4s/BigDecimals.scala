package com.sksamuel.avro4s

import org.apache.avro.SchemaBuilder

object BigDecimals {
  implicit val AsString: SchemaFor[BigDecimal] = SchemaFor[BigDecimal](SchemaBuilder.builder().stringType())
}