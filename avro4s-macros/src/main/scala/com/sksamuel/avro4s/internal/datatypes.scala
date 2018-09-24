package com.sksamuel.avro4s.internal

import com.sksamuel.avro4s.NamingStrategy
import org.apache.avro.{LogicalTypes, Schema}

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

case class NullableType(elementType: DataType) extends DataType with DataTypeSupport {

  import scala.collection.JavaConverters._

  override def toSchema(namingStrategy: NamingStrategy): Schema = {
    val schema = elementType.toSchema(namingStrategy)
    val schemas = Schema.create(Schema.Type.NULL) +: extractUnionSchemas(schema)
    Schema.createUnion(moveNullToHead(schemas).asJava)
  }
}