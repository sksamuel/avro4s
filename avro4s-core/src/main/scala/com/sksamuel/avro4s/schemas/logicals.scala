package com.sksamuel.avro4s.schemas

import org.apache.avro.{LogicalType, Schema}

object OffsetDateTimeLogicalType extends LogicalType("datetime-with-offset") {
  override def validate(schema: Schema): Unit = {
    super.validate(schema)
    if (schema.getType != Schema.Type.STRING) {
      throw new IllegalArgumentException("Logical type iso-datetime with offset must be backed by String")
    }
  }
}

object TimestampNanosLogicalType extends LogicalType("timestamp-nanos") {
  override def validate(schema: Schema): Unit = {
    super.validate(schema)
    if (schema.getType != Schema.Type.LONG) {
      throw new IllegalArgumentException("Logical type timestamp-nanos must be backed by long")
    }
  }
}