package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s._
import org.apache.avro.generic.GenericData
import org.apache.avro.{Conversions, LogicalTypes, SchemaBuilder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class WithBigDecimal(decimal: BigDecimal)
case class OptionalBigDecimal(big: Option[BigDecimal])

class BigDecimalDecoderTest extends AnyFlatSpec with Matchers {

  "Decoder" should "convert byte array to decimal" in {
    val schema = AvroSchema[WithBigDecimal]
    val record = new GenericData.Record(schema)
    val bytes = new Conversions.DecimalConversion().toBytes(BigDecimal(123.45).underlying(), null, LogicalTypes.decimal(8, 2))
    record.put("decimal", bytes)
    Decoder[WithBigDecimal].decode(schema).apply(record) shouldBe WithBigDecimal(BigDecimal(123.45))
  }

  it should "scale big decimals before decoding" in {
    given ScalePrecision = ScalePrecision(3, 8)
    val schema = AvroSchema[WithBigDecimal]
    val record = new GenericData.Record(schema)
    val bytes = new Conversions.DecimalConversion().toBytes(BigDecimal(12345.678).underlying(), null, LogicalTypes.decimal(8, 3))
    record.put("decimal", bytes)
    Decoder[WithBigDecimal].decode(schema).apply(record) shouldBe WithBigDecimal(BigDecimal(12345.678))
  }

  it should "support optional big decimals" in {
    val schema = AvroSchema[OptionalBigDecimal]
    val bytes =
      new Conversions.DecimalConversion().toBytes(BigDecimal(123.45).bigDecimal, null, LogicalTypes.decimal(8, 2))
    val record = new GenericData.Record(schema)
    record.put("big", bytes)
    Decoder[OptionalBigDecimal].decode(schema).apply(record) shouldBe OptionalBigDecimal(Option(BigDecimal(123.45)))

    val emptyRecord = new GenericData.Record(schema)
    emptyRecord.put("big", null)
    Decoder[OptionalBigDecimal].decode(schema).apply(emptyRecord) shouldBe OptionalBigDecimal(None)
  }

  it should "be able to decode strings as bigdecimals based on the schema" in {
    given SchemaFor[BigDecimal] = BigDecimals.AsString
    val schema = AvroSchema[BigDecimal]
    Decoder[BigDecimal].decode(schema).apply("123.45") shouldBe BigDecimal(123.45)
  }

  it should "be able to decode generic fixed as bigdecimals" in {
    given SchemaFor[BigDecimal] = SchemaFor[BigDecimal](LogicalTypes.decimal(10, 8).addToSchema(SchemaBuilder.fixed("BigDecimal").size(8)))
    val schema = AvroSchema[BigDecimal]
    val fixed = GenericData.get().createFixed(null, Array[Byte](0, 4, 98, -43, 55, 43, -114, 0), schema)
    Decoder[BigDecimal].decode(schema).apply(fixed) shouldBe BigDecimal(12345678)
  }

  //  it should "be able to decode longs as bigdecimals" in {
  //    val schema = LogicalTypes.decimal(5, 2).addToSchema(SchemaBuilder.builder().longType())
  //    BigDecimalDecoder.decode(12345, schema) shouldBe ""
  //    BigDecimalDecoder.decode(9999, schema) shouldBe ""
  //    BigDecimalDecoder.decode(java.lang.Long.valueOf(99887766), schema) shouldBe ""
  //    BigDecimalDecoder.decode(java.lang.Integer.valueOf(654), schema) shouldBe ""
  //  }
}
