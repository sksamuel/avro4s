package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder, Encoder, SchemaFor}
import com.sksamuel.avro4s.SchemaFor.StringSchemaFor
import org.apache.avro.generic.{GenericData, GenericFixed}
import org.apache.avro.{Conversions, LogicalTypes, Schema}
import org.scalatest.{FlatSpec, Matchers}

case class WithBigDecimal(decimal: BigDecimal)
case class OptionalBigDecimal(big: Option[BigDecimal])

class BigDecimalDecoderTest extends FlatSpec with Matchers {

  "Decoder" should "convert byte array to decimal" in {
    val schema = AvroSchema[WithBigDecimal]
    val record = new GenericData.Record(schema)
    val bytes = new Conversions.DecimalConversion().toBytes(BigDecimal(123.45).bigDecimal, null, LogicalTypes.decimal(8, 2))
    record.put("decimal", bytes)
    Decoder[WithBigDecimal].decode(record, schema) shouldBe WithBigDecimal(BigDecimal(123.45))
  }

  it should "support optional big decimals" in {
    val schema = AvroSchema[OptionalBigDecimal]
    val bytes = new Conversions.DecimalConversion().toBytes(BigDecimal(123.45).bigDecimal, null, LogicalTypes.decimal(8, 2))
    val record = new GenericData.Record(schema)
    record.put("big", bytes)
    Decoder[OptionalBigDecimal].decode(record, schema) shouldBe OptionalBigDecimal(Option(BigDecimal(123.45)))

    val emptyRecord = new GenericData.Record(schema)
    emptyRecord.put("big", null)
    Decoder[OptionalBigDecimal].decode(emptyRecord, schema) shouldBe OptionalBigDecimal(None)
  }

  it should "be able to decode strings as bigdecimals" in {
    implicit object BigDecimalAsString extends SchemaFor[BigDecimal] {
      override def schema: Schema = StringSchemaFor.schema
    }
    Decoder[BigDecimal].decode("123.45", BigDecimalAsString.schema) shouldBe BigDecimal(123.45)
  }

  it should "be able to decode generic fixed as bigdecimals" in {
    implicit object BigDecimalAsFixed extends SchemaFor[BigDecimal] {
      override def schema: Schema = LogicalTypes.decimal(10, 8).addToSchema(
        Schema.createFixed("BigDecimal", null, null, 8))
    }

    val fixed = GenericData.get().createFixed(null, Array[Byte](0, 4, 98, -43, 55, 43, -114, 0), BigDecimalAsFixed.schema)
    Decoder[BigDecimal].decode(fixed, BigDecimalAsFixed.schema) shouldBe BigDecimal(12345678)
  }
}