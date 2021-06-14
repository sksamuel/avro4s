package com.sksamuel.avro4s

import java.nio.ByteBuffer

import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.generic.GenericFixed
import org.apache.avro.{Conversions, Schema, SchemaBuilder}

import scala.math.BigDecimal.RoundingMode
import scala.math.BigDecimal.RoundingMode.RoundingMode

trait BigDecimalDecoders:
  given Decoder[BigDecimal] = new Decoder[BigDecimal] :
    override def decode(schema: Schema): Any => BigDecimal = {
      schema.getType match {
        case Schema.Type.BYTES => BigDecimalBytesDecoder.decode(schema)
        case Schema.Type.STRING => BigDecimalStringDecoder.decode(schema)
        case Schema.Type.FIXED => BigDecimalFixedDecoder.decode(schema)
        case t =>
          throw new Avro4sConfigurationException(
            s"Unable to create Decoder with schema type $t, only bytes, fixed, and string supported")
      }
    }

object BigDecimalBytesDecoder extends Decoder[BigDecimal] {
  override def decode(schema: Schema): Any => BigDecimal = {
    require(schema.getType == Schema.Type.BYTES)

    val logical = schema.getLogicalType.asInstanceOf[Decimal]
    val converter = new Conversions.DecimalConversion()
    val rm = java.math.RoundingMode.HALF_UP

    { value =>
      value match {
        case bb: ByteBuffer => converter.fromBytes(bb, schema, logical)
        case bytes: Array[Byte] => converter.fromBytes(ByteBuffer.wrap(bytes), schema, logical)
        case _ => throw new Avro4sDecodingException(s"Unable to decode '$value' to BigDecimal via ByteBuffer", value)
      }
    }
  }
}


object BigDecimalStringDecoder extends Decoder[BigDecimal] {
  override def decode(schema: Schema): Any => BigDecimal = {
    val decode = Decoder[String].decode(schema)
    { value => BigDecimal(decode(value)) }
  }
}

object BigDecimalFixedDecoder extends Decoder[BigDecimal] {
  override def decode(schema: Schema): Any => BigDecimal = {
    require(schema.getType == Schema.Type.FIXED)

    val logical = schema.getLogicalType.asInstanceOf[Decimal]
    val converter = new Conversions.DecimalConversion()
    val rm = java.math.RoundingMode.HALF_UP

    { value =>
      value match {
        case f: GenericFixed => converter.fromFixed(f, schema, logical)
        case _ => throw new Avro4sDecodingException(s"Unable to decode $value to BigDecimal via GenericFixed", value)
      }
    }
  }

}


