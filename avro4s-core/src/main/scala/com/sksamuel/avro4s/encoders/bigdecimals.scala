package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{Avro4sConfigurationException, Encoder}
import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.{Conversions, Schema}

trait BigDecimalEncoders:
  given Encoder[BigDecimal] = new Encoder[BigDecimal] :
    override def encode(schema: Schema): BigDecimal => AnyRef = {
      schema.getType match {
        case Schema.Type.BYTES => BigDecimalBytesEncoder.encode(schema)
        case Schema.Type.STRING => BigDecimalStringEncoder.encode(schema)
        case Schema.Type.FIXED => BigDecimalFixedEncoder.encode(schema)
        case t =>
          throw new Avro4sConfigurationException(
            s"Unable to create Encoder with schema type $t, only bytes, fixed, and string supported")
      }
    }

/**
  * An [[Encoder]] for [[BigDecimal]] that encodes as byte arrays.
  */
object BigDecimalBytesEncoder extends Encoder[BigDecimal] {

  override def encode(schema: Schema): BigDecimal => AnyRef = {
    require(schema.getType == Schema.Type.BYTES)

    val logical = schema.getLogicalType.asInstanceOf[Decimal]
    val converter = new Conversions.DecimalConversion()
    val rm = java.math.RoundingMode.HALF_UP

    { value => converter.toBytes(value.underlying().setScale(logical.getScale, rm), schema, logical) }
  }
}

/**
  * An [[Encoder]] for [[BigDecimal]] that encodes as Strings.
  */
object BigDecimalStringEncoder extends Encoder[BigDecimal] {

  override def encode(schema: Schema): BigDecimal => AnyRef = {
    require(schema.getType == Schema.Type.STRING)

    val logical = schema.getLogicalType.asInstanceOf[Decimal]
    val converter = new Conversions.DecimalConversion()
    val encode = Encoder[String].contramap[BigDecimal](_.toString).encode(schema)

    { value => encode(value) }
  }
}

/**
  * An [[Encoder]] for [[BigDecimal]] that encodes as fixed size byte arrays.
  */
object BigDecimalFixedEncoder extends Encoder[BigDecimal] {

  override def encode(schema: Schema): BigDecimal => AnyRef = {
    require(schema.getType == Schema.Type.FIXED)

    val logical = schema.getLogicalType.asInstanceOf[Decimal]
    val converter = new Conversions.DecimalConversion()
    val rm = java.math.RoundingMode.HALF_UP

    { value => converter.toFixed(value.underlying().setScale(logical.getScale, rm), schema, logical) }
  }
}
