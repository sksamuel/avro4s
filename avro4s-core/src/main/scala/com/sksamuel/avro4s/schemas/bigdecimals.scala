package com.sksamuel.avro4s

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

case class ScalePrecision(scale: Int, precision: Int)

object ScalePrecision {
  given default: ScalePrecision = ScalePrecision(2, 8)
}

trait BigDecimalSchemas:
  given(using sp: ScalePrecision): SchemaFor[BigDecimal] = new BigDecimalSchemaFor(sp)

class BigDecimalSchemaFor(sp: ScalePrecision) extends SchemaFor[BigDecimal] :
  override def schema: Schema = LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(SchemaBuilder.builder.bytesType)

object BigDecimals {
  val AsString: SchemaFor[BigDecimal] = SchemaFor[BigDecimal](SchemaBuilder.builder.stringType)
}
