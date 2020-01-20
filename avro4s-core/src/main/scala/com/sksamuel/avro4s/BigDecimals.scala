package com.sksamuel.avro4s

import magnolia.TypeName
import org.apache.avro.{Schema, SchemaBuilder}

object BigDecimals {
  implicit object AsString extends SchemaFor[BigDecimal] {
    override def schema(fieldMapper: FieldMapper, parents: Map[Class[_], Schema]): Schema = SchemaBuilder.builder().stringType()
  }
}