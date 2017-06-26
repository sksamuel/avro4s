package com.sksamuel.avro4s

import org.apache.avro.{AvroRuntimeException, Schema, SchemaBuilder}
import org.scalatest.{FlatSpec, Matchers}

case class WithBigDecimal(decimal: BigDecimal)

class ToRecordTest extends FlatSpec with Matchers {

  "ToRecord" should "use byte array for decimal" in {
    val obj = WithBigDecimal(12.34)
    val record = ToRecord[WithBigDecimal](obj)
    record.toString shouldBe """{"decimal": {"bytes": "12.34"}}"""
  }

  "ToRecord with the derived schema passed in" should "result in the same" in {
    val obj = WithBigDecimal(12.34)
    val schemaFor = SchemaFor[WithBigDecimal]
    val record = ToRecord.withSchemaFor[WithBigDecimal](schemaFor)(obj)
    record.toString shouldBe """{"decimal": {"bytes": "12.34"}}"""
  }

  "ToRecord with custom schema" should "use the custom schema" in {
    val obj = WithBigDecimal(12.34)
    val schemaFor = new SchemaFor[WithBigDecimal] {
      override def apply(): Schema =
        SchemaBuilder
          .record("WithBigDecimal")
          .fields()
          .endRecord()
    }

    val noSchemaDecimalField = intercept[AvroRuntimeException](ToRecord.withSchemaFor[WithBigDecimal](schemaFor)(obj))
    noSchemaDecimalField.getMessage shouldBe "Not a valid schema field: decimal"
  }
}
