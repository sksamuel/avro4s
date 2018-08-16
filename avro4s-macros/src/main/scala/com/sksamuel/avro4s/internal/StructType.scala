package com.sksamuel.avro4s.internal

case class Anno(name: String, args: Seq[Any])

sealed trait DataType

case object StringType extends DataType
case object LongType extends DataType
case object IntType extends DataType
case object ShortType extends DataType
case object ByteType extends DataType
case object BooleanType extends DataType
case object BinaryType extends DataType
case object UUIDType extends DataType

case object DoubleType extends DataType
case object FloatType extends DataType

case object LocalDateType extends DataType
case object LocalDateTimeType extends DataType
case object LocalTimeType extends DataType
case object TimestampType extends DataType

case class NullableType(elementType: DataType) extends DataType

case class DecimalType(precision: Int, scale: Int) extends DataType

case class MapType(keyType: DataType, valueType: DataType) extends DataType
case class ArrayType(valueType: DataType) extends DataType

case class EnumType(className: String,
                    simpleName: String,
                    packageName: String,
                    symbols: Seq[String],
                    annotations: Seq[Anno]) extends DataType

case class UnionType(types: Seq[DataType]) extends DataType

case class StructType(qualifiedName: String,
                      simpleName: String,
                      packageName: String,
                      annotations: Seq[Anno],
                      fields: Seq[StructField]) extends DataType

case class StructField(name: String,
                       dataType: DataType,
                       annotations: Seq[Anno] = Nil,
                       default: Any = null)