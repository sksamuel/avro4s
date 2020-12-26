package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.encoders.Avro4sDecodingException
import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.generic.GenericFixed
import org.apache.avro.{Conversions, Schema}

import java.nio.ByteBuffer
import java.util.UUID

trait BigDecimalDecoders {

  given DecoderFor[BigDecimal] = new DecoderFor[BigDecimal] {
    override def decoder(schema: Schema): Decoder[BigDecimal] = schema.getType match {
      case Schema.Type.FIXED => BigDecimalFixedDecoder(schema)
      case Schema.Type.BYTES => BigDecimalBytesDecoder(schema)
      case Schema.Type.STRING => BigDecimalStringDecoder
    }
  }

}

class BigDecimalBytesDecoder(schema: Schema) extends Decoder[BigDecimal] {
  private val converter = new Conversions.DecimalConversion
  private val decimal = schema.getLogicalType.asInstanceOf[Decimal]
  def decode(value: Any): BigDecimal = value match {
    case f: ByteBuffer => converter.fromBytes(f, schema, decimal)
    case f: Array[Byte] => converter.fromBytes(ByteBuffer.wrap(f), schema, decimal)
  }
}

class BigDecimalFixedDecoder(schema: Schema) extends Decoder[BigDecimal] {
  private val converter = new Conversions.DecimalConversion
  private val decimal = schema.getLogicalType.asInstanceOf[Decimal]
  def decode(value: Any): BigDecimal = value match {
    case f: GenericFixed => converter.fromFixed(f, schema, decimal)
  }
}

object BigDecimalStringDecoder extends Decoder[BigDecimal] {
  override def decode(value: Any): BigDecimal = BigDecimal(StringDecoder.decode(value))
}