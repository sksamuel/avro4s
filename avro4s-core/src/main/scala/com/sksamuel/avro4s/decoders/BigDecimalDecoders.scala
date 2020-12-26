package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.{DefaultFieldMapper, FieldMapper}
import com.sksamuel.avro4s.encoders.Avro4sDecodingException
import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.generic.GenericFixed
import org.apache.avro.util.Utf8
import org.apache.avro.{Conversions, Schema}

import java.nio.ByteBuffer
import java.util.UUID

trait BigDecimalDecoders {

  given Decoder[BigDecimal] = new Decoder[BigDecimal] {
    private val converter = new Conversions.DecimalConversion
    override def decode(schema: Schema, mapper: FieldMapper): Any => BigDecimal = {
      val decimal = schema.getLogicalType.asInstanceOf[Decimal]
      schema.getType match {
        case Schema.Type.FIXED => { value => value match {
          case f: GenericFixed => converter.fromFixed(f, schema, decimal)
        }}
        case Schema.Type.BYTES => { value => value match {
          case f: ByteBuffer => converter.fromBytes(f, schema, decimal)
          case f: Array[Byte] => converter.fromBytes(ByteBuffer.wrap(f), schema, decimal)
        }}
        case Schema.Type.STRING => { value => value match {
          case s: CharSequence => BigDecimal(s.toString)
        }}
      }
    }
  }
}