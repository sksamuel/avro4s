package com.sksamuel.avro4s

import org.apache.avro.{LogicalTypes, SchemaBuilder}

case class ScalePrecision(scale: Int, precision: Int)

object ScalePrecision {
  given default: ScalePrecision = ScalePrecision(2, 8)
}

trait BigDecimalSchemas:
  given BigDecimalSchemaFor(using sp: ScalePrecision = ScalePrecision.default): SchemaFor[BigDecimal] =
    SchemaFor(LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(SchemaBuilder.builder.bytesType))

object BigDecimals {

  //  private[avro4s] trait BigDecimalConversion[Typeclass[_]]
  //    extends SchemaAware[Typeclass, BigDecimal]
  //      with Serializable {
  //
  //    def roundingMode: RoundingMode

  //  @transient protected lazy val decimal = schema.getLogicalType.asInstanceOf[Decimal]
  //  @transient protected lazy val converter = new Conversions.DecimalConversion
  //  protected val rm = java.math.RoundingMode.valueOf(roundingMode.id)
  //}

  given AsString: SchemaFor[BigDecimal] =
    SchemaFor[BigDecimal](SchemaBuilder.builder.stringType)
}
