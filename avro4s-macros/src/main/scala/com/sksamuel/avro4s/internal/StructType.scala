package com.sksamuel.avro4s.internal

case class Annotation(name: String, args: Seq[Any])

sealed trait DataType

case object StringType extends DataType
case object LongType extends DataType
case object IntType extends DataType
case object ShortType extends DataType
case object ByteType extends DataType
case object BooleanType extends DataType
case object BinaryType extends DataType
case object BigDecimalType extends DataType
case object UUIDType extends DataType
case object DoubleType extends DataType
case object FloatType extends DataType

case class StructType(className: String,
                      annotations: Seq[Annotation],
                      fields: Seq[StructField]) extends DataType

case class StructField(name: String,
                       dataType: DataType,
                       annotations: Seq[Annotation],
                       default: Any)