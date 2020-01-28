package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s._
import org.apache.avro.generic.GenericData
import org.apache.avro.{Conversions, LogicalTypes, SchemaBuilder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class WithBigDecimal(decimal: BigDecimal)
case class OptionalBigDecimal(big: Option[BigDecimal])

class BigDecimalDecoderTest extends AnyFlatSpec with Matchers {

  implicit val fm: FieldMapper = DefaultFieldMapper

  "Decoder" should "convert byte array to decimal" in {
    val schema = AvroSchemaV2[WithBigDecimal]
    val record = new GenericData.Record(schema)
    val bytes =
      new Conversions.DecimalConversion().toBytes(BigDecimal(123.45).bigDecimal, null, LogicalTypes.decimal(8, 2))
    record.put("decimal", bytes)
    Decoder[WithBigDecimal].decode(record) shouldBe WithBigDecimal(BigDecimal(123.45))
  }

  it should "support optional big decimals" in {
    val schema = AvroSchemaV2[OptionalBigDecimal]
    val bytes =
      new Conversions.DecimalConversion().toBytes(BigDecimal(123.45).bigDecimal, null, LogicalTypes.decimal(8, 2))
    val record = new GenericData.Record(schema)
    record.put("big", bytes)
    Decoder[OptionalBigDecimal].decode(record) shouldBe OptionalBigDecimal(Option(BigDecimal(123.45)))

    val emptyRecord = new GenericData.Record(schema)
    emptyRecord.put("big", null)
    Decoder[OptionalBigDecimal].decode(emptyRecord) shouldBe OptionalBigDecimal(None)
  }

  it should "be able to decode strings as bigdecimals" in {
    val schemaFor = SchemaForV2[BigDecimal](SchemaBuilder.builder.stringType)
    Decoder[BigDecimal].withSchema(schemaFor).decode("123.45") shouldBe BigDecimal(123.45)
  }

  it should "be able to decode generic fixed as bigdecimals" in {
    val schemaFor = SchemaForV2[BigDecimal](
      LogicalTypes.decimal(10, 8).addToSchema(SchemaBuilder.fixed("BigDecimal").size(8))
    )

    val fixed =
      GenericData.get().createFixed(null, Array[Byte](0, 4, 98, -43, 55, 43, -114, 0), schemaFor.schema)
    Decoder[BigDecimal].withSchema(schemaFor).decode(fixed) shouldBe BigDecimal(12345678)
  }

//  it should "be able to decode longs as bigdecimals" in {
//    val schema = LogicalTypes.decimal(5, 2).addToSchema(SchemaBuilder.builder().longType())
//    BigDecimalDecoder.decode(12345, schema) shouldBe ""
//    BigDecimalDecoder.decode(9999, schema) shouldBe ""
//    BigDecimalDecoder.decode(java.lang.Long.valueOf(99887766), schema) shouldBe ""
//    BigDecimalDecoder.decode(java.lang.Integer.valueOf(654), schema) shouldBe ""
//  }
}
