package com.sksamuel.avro4s

import com.sksamuel.avro4s.internal.SchemaFor
import org.apache.avro.{Schema, SchemaBuilder}

object BigDecimalAsString {
  implicit object BigDecimalAsStringCodec extends SchemaFor[BigDecimal] {
    override def schema: Schema = SchemaBuilder.builder().stringType()
  }
}

object BigDecimalAsFixed {
  implicit object BigDecimalAsFixedCodec extends SchemaFor[BigDecimal] {
    override def schema: Schema = Schema.createFixed("myname", null, "namespace", 55)
  }
}
