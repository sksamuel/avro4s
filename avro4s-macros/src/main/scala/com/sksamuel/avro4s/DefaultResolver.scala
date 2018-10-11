package com.sksamuel.avro4s

import java.nio.ByteBuffer

import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.avro.{Conversions, Schema}

/**
  * When we set a default on an avro field, the type must match
  * the schema definition. For example, if our field has a schema
  * of type Long, then the default must be a number value.
  *
  * This class will accept an Avro encoded value and convert it
  * to a suitable default type.
  */
object DefaultResolver {
  def apply(value: AnyRef, schema: Schema): AnyRef = value match {
    case u: Utf8 => u.toString
    case enum: GenericData.EnumSymbol => enum.toString
    case fixed: GenericData.Fixed => fixed.bytes()
    case byteBuffer: ByteBuffer if schema.getLogicalType.isInstanceOf[Decimal] =>
      val decimalConversion = new Conversions.DecimalConversion
      val bd = decimalConversion.fromBytes(byteBuffer, schema, schema.getLogicalType)
      java.lang.Double.valueOf(bd.doubleValue)
    case byteBuffer: ByteBuffer => byteBuffer.array()
    case _ => value
  }
}
