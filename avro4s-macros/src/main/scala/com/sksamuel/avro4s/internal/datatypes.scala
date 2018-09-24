package com.sksamuel.avro4s.internal

import com.sksamuel.avro4s.NamingStrategy
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

import scala.util.{Failure, Success, Try}

/**
  * Represents a Scala or Java annotation on a class or field.
  *
  * @param className the classname of the annotation type, eg com.sksamuel.avro4s.AvroProp
  * @param args      the arguments to the annotation instance
  */
case class Anno(className: String, args: Seq[Any])

sealed trait DataType {

  /**
    * Returns the Avro [[Schema]] representation of this [[DataType]].
    */
  def toSchema(namingStrategy: NamingStrategy): Schema
}

trait DataTypeSupport {

  import scala.collection.JavaConverters._

  /**
    * Avro union schemas can't contain other union schemas as a direct
    * child. So whenever we create a union schema, we ned to check if
    * our children are unions, and if they are, merge into the parent union.
    */
  def extractUnionSchemas(schema: Schema): Seq[Schema] = Try(schema.getTypes /* throws an error if we're not a union */) match {
    case Success(subschemas) => subschemas.asScala.flatMap(extractUnionSchemas)
    case Failure(_) => Seq(schema)
  }

  // for a union the type that has a default must be first,
  // if there is no default, then null is first by convention
  def moveNullToHead(schemas: Seq[Schema]) = {
    val (nulls, withoutNull) = schemas.partition(_.getType == Schema.Type.NULL)
    nulls.headOption.toSeq ++ withoutNull
  }
}

/**
  * Implementations of this [[DataType]] always return the same
  * constant schema.
  */
abstract class ConstDataType(schema: Schema) extends DataType {
  override def toSchema(namingStrategy: NamingStrategy): Schema = schema
}

sealed trait LogicalDataType extends DataType

case object BinaryType extends ConstDataType(Schema.create(Schema.Type.BYTES))
case object BooleanType extends ConstDataType(Schema.create(Schema.Type.BOOLEAN))
case object ByteType extends ConstDataType(Schema.create(Schema.Type.INT))
case object DoubleType extends ConstDataType(Schema.create(Schema.Type.DOUBLE))
case object FloatType extends ConstDataType(Schema.create(Schema.Type.FLOAT))
case object IntType extends ConstDataType(Schema.create(Schema.Type.INT))
case object LongType extends ConstDataType(Schema.create(Schema.Type.LONG))
case object ShortType extends ConstDataType(Schema.create(Schema.Type.INT))
case object StringType extends ConstDataType(Schema.create(Schema.Type.STRING))

case class DecimalType(precision: Int, scale: Int) extends LogicalDataType {
  override def toSchema(namingStrategy: NamingStrategy): Schema = {
    val schema = Schema.create(Schema.Type.BYTES)
    LogicalTypes.decimal(precision, scale).addToSchema(schema)
    schema
  }
}

case class FixedType(name: String, namespace: Option[String], size: Int) extends DataType {
  override def toSchema(namingStrategy: NamingStrategy): Schema = Schema.createFixed(name, null, namespace.orNull, size)
}

object FixedType {
  def apply(name: String, size: Int): FixedType = FixedType(name, None, size)
}

case object UUIDType extends LogicalDataType {
  private val schema = Schema.create(Schema.Type.STRING)
  LogicalTypes.uuid().addToSchema(schema)
  override def toSchema(namingStrategy: NamingStrategy): Schema = schema
}

case object LocalDateType extends LogicalDataType {
  private val schema = Schema.create(Schema.Type.INT)
  LogicalTypes.date().addToSchema(schema)
  override def toSchema(namingStrategy: NamingStrategy): Schema = schema
}

case object LocalDateTimeType extends LogicalDataType {
  private val schema = Schema.create(Schema.Type.LONG)
  LogicalTypes.timestampMillis().addToSchema(schema)
  override def toSchema(namingStrategy: NamingStrategy): Schema = schema
}

case object LocalTimeType extends LogicalDataType {
  private val schema = Schema.create(Schema.Type.INT)
  LogicalTypes.timeMillis().addToSchema(schema)
  override def toSchema(namingStrategy: NamingStrategy): Schema = schema
}

case object TimestampType extends LogicalDataType {
  private val schema = Schema.create(Schema.Type.LONG)
  LogicalTypes.timestampMillis().addToSchema(schema)
  override def toSchema(namingStrategy: NamingStrategy): Schema = schema
}

case class NullableType(elementType: DataType) extends DataType with DataTypeSupport {

  import scala.collection.JavaConverters._

  override def toSchema(namingStrategy: NamingStrategy): Schema = {
    val schema = elementType.toSchema(namingStrategy)
    val schemas = Schema.create(Schema.Type.NULL) +: extractUnionSchemas(schema)
    Schema.createUnion(moveNullToHead(schemas).asJava)
  }
}

case class MapType(keyType: DataType, valueType: DataType) extends DataType {
  override def toSchema(namingStrategy: NamingStrategy): Schema = Schema.createMap(valueType.toSchema(namingStrategy))
}

case class ArrayType(valueType: DataType) extends DataType {
  override def toSchema(namingStrategy: NamingStrategy): Schema = SchemaBuilder.array().items(valueType.toSchema(namingStrategy))
}

case class EnumType(className: String,
                    simpleName: String,
                    packageName: String,
                    symbols: Seq[String],
                    annotations: Seq[Anno]) extends DataType {
  private val extractor = new AnnotationExtractors(annotations)
  private val namespace = extractor.namespace.getOrElse(packageName)
  private val schema = SchemaBuilder.enumeration(simpleName).namespace(namespace).symbols(symbols: _*)
  override def toSchema(namingStrategy: NamingStrategy): Schema = schema
}

/**
  * Represents a type that can be one of several different underlying types.
  */
case class UnionType(types: Seq[DataType]) extends DataType with DataTypeSupport {

  import scala.collection.JavaConverters._

  override def toSchema(namingStrategy: NamingStrategy): Schema = {
    val schemas = types.map(_.toSchema(namingStrategy)).flatMap(extractUnionSchemas)
    Schema.createUnion(moveNullToHead(schemas).asJava)
  }
}

case class StructType(qualifiedName: String,
                      simpleName: String,
                      packageName: String,
                      annotations: Seq[Anno],
                      fields: Seq[StructField],
                      valueType: Boolean) extends DataType {
  override def toSchema(namingStrategy: NamingStrategy): Schema = StructSchemaEncoder.encode(this, namingStrategy)
}

case class StructField(name: String,
                       dataType: DataType,
                       annotations: Seq[Anno] = Nil,
                       default: Any = null)