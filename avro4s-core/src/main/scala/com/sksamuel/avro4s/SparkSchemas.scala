package com.sksamuel.avro4s

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

import scala.language.implicitConversions

object SparkSchemas {

  // see https://github.com/sksamuel/avro4s/issues/271
  implicit def BigDecimalSchemaFor(sp: ScalePrecision) = new SchemaFor[BigDecimal] {
    /**
      * To be precise Spark expect the following mapping type -> precision:
      *
      * INT32 => 0 <= precision <= 9
      * INT64 => 9 < precision <= 18
      * BINARY => 18 < precision
      * FIXED_LEN_BYTE_ARRAY => 0 <= precision
      * See:
      * https://github.com/apache/spark/blob/v2.4.0/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L211-L242
      * https://github.com/apache/spark/blob/v2.4.0/sql/catalyst/src/main/scala/org/apache/spark/sql/types/Decimal.scala#L417-L421
      * https://github.com/apache/spark/blob/v2.4.0/sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedColumnReader.java#L501-L538
      */
    override def schema(implicit namingStrategy: NamingStrategy): Schema = {
      if (0 <= sp.precision && sp.precision <= 9) {
        LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(SchemaBuilder.builder.intType)
      } else if (10 <= sp.precision && sp.precision <= 18) {
        LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(SchemaBuilder.builder.longType)
      } else {
        LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(SchemaBuilder.builder.bytesType)
      }
    }
  }

}
