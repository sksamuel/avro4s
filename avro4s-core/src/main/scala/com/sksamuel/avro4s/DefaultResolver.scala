package com.sksamuel.avro4s

import scala.collection.JavaConverters._
import java.nio.ByteBuffer
import java.util.UUID

import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.{Conversions, Schema}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

/**
  * When we set a default on an avro field, the type must match
  * the schema definition. For example, if our field has a schema
  * of type UUID, then the default must be a String, or for a schema
  * of Long, then the type must be a java Long and not a Scala long.
  *
  * This class will accept a scala value and convert it into a type
  * suitable for Avro and the provided schema.
  */
object DefaultResolver {
  def apply(value: Any, schema: Schema): AnyRef = value match {
    case Some(x) => apply(x, schema)
    case u: Utf8 => u.toString
    case uuid: UUID => uuid.toString
    case enum: GenericData.EnumSymbol => enum.toString
    case fixed: GenericData.Fixed => fixed.bytes()
    case bd: BigDecimal => bd.toString()
    case byteBuffer: ByteBuffer if schema.getLogicalType.isInstanceOf[Decimal] =>
      val decimalConversion = new Conversions.DecimalConversion
      val bd = decimalConversion.fromBytes(byteBuffer, schema, schema.getLogicalType)
      java.lang.Double.valueOf(bd.doubleValue)
    case byteBuffer: ByteBuffer => byteBuffer.array()
    case x: scala.Long => java.lang.Long.valueOf(x)
    case x: scala.Boolean => java.lang.Boolean.valueOf(x)
    case x: scala.Int => java.lang.Integer.valueOf(x)
    case x: scala.Double => java.lang.Double.valueOf(x)
    case x: scala.Float => java.lang.Float.valueOf(x)
    case x: Map[_,_] => x.asJava
    case x: Seq[_] => x.asJava
    case _ => value.asInstanceOf[AnyRef]
  }

}
