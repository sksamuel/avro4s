package com.sksamuel.avro4s.internal

/**
  * Representations a Scala or Java annotation on a class or field.
  *
  * @param className the classname of the annotation type, eg com.sksamuel.avro4s.AvroProp
  * @param args      the arguments to the annotation instance
  */
case class Anno(className: String, args: Seq[Any])

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

/**
  * Represents a type that can be one of several different underlying types.
  */
case class UnionType(types: Seq[DataType]) extends DataType

object UnionType {

  /**
    * Takes a list of data types and creates a union from them, flattening any nested union types
    */
  def flatten(dataTypes: DataType*): UnionType = {
    val types = dataTypes.flatMap {
      case UnionType(tps) => tps
      case other => Seq(other)
    }
    UnionType(types)
  }
}

case class StructType(qualifiedName: String,
                      simpleName: String,
                      packageName: String,
                      annotations: Seq[Anno],
                      fields: Seq[StructField],
                      valueType: Boolean) extends DataType

case class StructField(name: String,
                       dataType: DataType,
                       annotations: Seq[Anno] = Nil,
                       default: Any = null)