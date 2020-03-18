package com.sksamuel.avro4s

import org.apache.avro.SchemaBuilder

object BigDecimals {
  implicit val AsString: SchemaForV2[BigDecimal] = SchemaForV2[BigDecimal](SchemaBuilder.builder.stringType)
}