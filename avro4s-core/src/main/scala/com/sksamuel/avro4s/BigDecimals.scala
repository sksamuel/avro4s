//package com.sksamuel.avro4s
//
//import java.nio.ByteBuffer
//
//import com.sksamuel.avro4s.BigDecimals.BigDecimalConversion
//import org.apache.avro.LogicalTypes.Decimal
//import org.apache.avro.generic.GenericFixed
//import org.apache.avro.{Conversions, Schema, SchemaBuilder}
//
//import scala.math.BigDecimal.RoundingMode
//import scala.math.BigDecimal.RoundingMode.RoundingMode
//
//trait BigDecimalDecoders {
//
//  implicit def bigDecimalDecoder(implicit scalePrecision: ScalePrecision = ScalePrecision.default,
//                                 roundingMode: RoundingMode = RoundingMode.UNNECESSARY): Decoder[BigDecimal] =
//    new BigDecimalBytesDecoder(SchemaFor.bigDecimalSchemaFor, roundingMode)
//
//  abstract class BigDecimalDecoderBase(roundingMode: RoundingMode) extends Decoder[BigDecimal] {
//
//    override def withSchema(schemaFor: SchemaFor[BigDecimal]): Decoder[BigDecimal] =
//      schemaFor.schema.getType match {
//        case Schema.Type.BYTES  => new BigDecimalBytesDecoder(schemaFor, roundingMode)
//        case Schema.Type.STRING => new BigDecimalStringDecoder(schemaFor, roundingMode)
//        case Schema.Type.FIXED  => new BigDecimalFixedDecoder(schemaFor, roundingMode)
//        case t =>
//          throw new Avro4sConfigurationException(
//            s"Unable to create Decoder with schema type $t, only bytes, fixed, and string supported")
//      }
//  }
//
//  class BigDecimalBytesDecoder(val schemaFor: SchemaFor[BigDecimal], val roundingMode: RoundingMode)
//      extends BigDecimalDecoderBase(roundingMode)
//      with BigDecimalConversion[Decoder] {
//
//    def decode(value: Any): BigDecimal = value match {
//      case bb: ByteBuffer => converter.fromBytes(bb, schema, decimal)
//      case _ =>
//        throw new Avro4sDecodingException(s"Unable to decode '$value' to BigDecimal via ByteBuffer", value, this)
//    }
//  }
//
//  class BigDecimalStringDecoder(val schemaFor: SchemaFor[BigDecimal], roundingMode: RoundingMode)
//      extends BigDecimalDecoderBase(roundingMode) {
//
//    def decode(value: Any): BigDecimal = BigDecimal(Decoder.StringDecoder.decode(value))
//  }
//
//  class BigDecimalFixedDecoder(val schemaFor: SchemaFor[BigDecimal], val roundingMode: RoundingMode)
//      extends BigDecimalDecoderBase(roundingMode)
//      with BigDecimalConversion[Decoder] {
//
//    def decode(value: Any): BigDecimal = value match {
//      case f: GenericFixed => converter.fromFixed(f, schema, decimal)
//      case _ =>
//        throw new Avro4sDecodingException(s"Unable to decode $value to BigDecimal via GenericFixed", value, this)
//    }
//  }
//}
//

