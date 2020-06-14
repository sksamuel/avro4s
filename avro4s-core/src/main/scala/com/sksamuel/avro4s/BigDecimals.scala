package com.sksamuel.avro4s

import java.nio.ByteBuffer

import com.sksamuel.avro4s.BigDecimals.BigDecimalConversion
import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.generic.GenericFixed
import org.apache.avro.{Conversions, Schema, SchemaBuilder}

import scala.math.BigDecimal.RoundingMode
import scala.math.BigDecimal.RoundingMode.RoundingMode

trait BigDecimalDecoders {

  implicit def bigDecimalDecoder(implicit scalePrecision: ScalePrecision = ScalePrecision.default,
                                 roundingMode: RoundingMode = RoundingMode.UNNECESSARY): Decoder[BigDecimal] =
    new BigDecimalBytesDecoder(SchemaFor.bigDecimalSchemaFor, roundingMode)

  abstract class BigDecimalDecoderBase(roundingMode: RoundingMode) extends Decoder[BigDecimal] {

    override def withSchema(schemaFor: SchemaFor[BigDecimal]): Decoder[BigDecimal] =
      schemaFor.schema.getType match {
        case Schema.Type.BYTES  => new BigDecimalBytesDecoder(schemaFor, roundingMode)
        case Schema.Type.STRING => new BigDecimalStringDecoder(schemaFor, roundingMode)
        case Schema.Type.FIXED  => new BigDecimalFixedDecoder(schemaFor, roundingMode)
        case t =>
          throw new Avro4sConfigurationException(
            s"Unable to create Decoder with schema type $t, only bytes, fixed, and string supported")
      }
  }

  class BigDecimalBytesDecoder(val schemaFor: SchemaFor[BigDecimal], val roundingMode: RoundingMode)
      extends BigDecimalDecoderBase(roundingMode)
      with BigDecimalConversion[Decoder] {

    def decode(value: Any): BigDecimal = value match {
      case bb: ByteBuffer => converter.fromBytes(bb, schema, decimal)
      case _ =>
        throw new Avro4sDecodingException(s"Unable to decode '$value' to BigDecimal via ByteBuffer", value, this)
    }
  }

  class BigDecimalStringDecoder(val schemaFor: SchemaFor[BigDecimal], roundingMode: RoundingMode)
      extends BigDecimalDecoderBase(roundingMode) {

    def decode(value: Any): BigDecimal = BigDecimal(Decoder.StringDecoder.decode(value))
  }

  class BigDecimalFixedDecoder(val schemaFor: SchemaFor[BigDecimal], val roundingMode: RoundingMode)
      extends BigDecimalDecoderBase(roundingMode)
      with BigDecimalConversion[Decoder] {

    def decode(value: Any): BigDecimal = value match {
      case f: GenericFixed => converter.fromFixed(f, schema, decimal)
      case _ =>
        throw new Avro4sDecodingException(s"Unable to decode $value to BigDecimal via GenericFixed", value, this)
    }
  }
}

trait BigDecimalEncoders {

  implicit def bigDecimalEncoder(implicit scalePrecision: ScalePrecision = ScalePrecision.default,
                                 roundingMode: RoundingMode = RoundingMode.UNNECESSARY): Encoder[BigDecimal] =
    new BigDecimalBytesEncoder(SchemaFor.bigDecimalSchemaFor, roundingMode)

  abstract class BigDecimalEncoderBase(roundingMode: RoundingMode) extends Encoder[BigDecimal] {

    override def withSchema(schemaFor: SchemaFor[BigDecimal]): Encoder[BigDecimal] =
      schemaFor.schema.getType match {
        case Schema.Type.BYTES  => new BigDecimalBytesEncoder(schemaFor, roundingMode)
        case Schema.Type.STRING => new BigDecimalStringEncoder(schemaFor, roundingMode)
        case Schema.Type.FIXED  => new BigDecimalFixedEncoder(schemaFor, roundingMode)
        case t =>
          throw new Avro4sConfigurationException(
            s"Unable to create Encoder with schema type $t, only bytes, fixed, and string supported")
      }
  }

  class BigDecimalBytesEncoder(val schemaFor: SchemaFor[BigDecimal], val roundingMode: RoundingMode)
      extends BigDecimalEncoderBase(roundingMode)
      with BigDecimalConversion[Encoder] {
    def encode(value: BigDecimal): AnyRef =
      converter.toBytes(value.underlying.setScale(decimal.getScale, rm), schema, decimal)
  }

  class BigDecimalStringEncoder(val schemaFor: SchemaFor[BigDecimal], roundingMode: RoundingMode)
      extends BigDecimalEncoderBase(roundingMode) {
    def encode(value: BigDecimal): AnyRef = Encoder.StringEncoder.encode(value.toString())
  }

  class BigDecimalFixedEncoder(val schemaFor: SchemaFor[BigDecimal], val roundingMode: RoundingMode)
      extends BigDecimalEncoderBase(roundingMode)
      with BigDecimalConversion[Encoder] {
    def encode(value: BigDecimal): AnyRef =
      converter.toFixed(value.underlying.setScale(decimal.getScale, rm), schema, decimal)
  }
}

object BigDecimals {

  private[avro4s] trait BigDecimalConversion[Typeclass[_]]
      extends SchemaAware[Typeclass, BigDecimal]
      with Serializable {

    def roundingMode: RoundingMode

    @transient protected lazy val decimal = schema.getLogicalType.asInstanceOf[Decimal]
    @transient protected lazy val converter = new Conversions.DecimalConversion
    protected val rm = java.math.RoundingMode.valueOf(roundingMode.id)
  }

  implicit val AsString: SchemaFor[BigDecimal] =
    SchemaFor[BigDecimal](SchemaBuilder.builder.stringType, DefaultFieldMapper)
}
