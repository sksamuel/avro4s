package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

object SchemaHelper {

  import scala.collection.JavaConverters._

  def extractTraitSubschema(typeName: String, schema: Schema): Schema = {
    import scala.collection.JavaConverters._
    require(schema.getType == Schema.Type.UNION, "Can only extract subschemas from a UNION")
    schema.getTypes.asScala
      .find(_.getFullName == typeName)
      .getOrElse(sys.error(s"Cannot find subschema for type name $typeName"))
  }

  /**
    * If the given schema is of the required type, returns the schema.
    * Otherwise if the given schema is a union, it will attempt to find the
    * required type in the union.
    * Finally, will throw.
    */
  def extractSchemaFromPossibleUnion(schema: Schema, `type`: Schema.Type): Schema = {
    import scala.collection.JavaConverters._
    schema.getType match {
      case `type` => schema
      case Schema.Type.UNION => schema.getTypes.asScala.find(_.getType == `type`).getOrElse(sys.error(s"Could not find a schema of type ${`type`} in union $schema"))
      case _ => sys.error(s"Require schema type ${`type`} but was given $schema")
    }
  }

  /**
    * Requires a UNION schema and will attempt to find a subschema that
    * matches the default value type. If one matches it will move that
    * schema to the head, as required by the spec.
    */
  def moveDefaultToHead(schema: Schema, default: Any): Schema = {
    require(schema.getType == Schema.Type.UNION)
    val defaultType = default match {
      case _: String => Schema.Type.STRING
      case _: Utf8 => Schema.Type.STRING
      case _: Long => Schema.Type.LONG
      case _: Int => Schema.Type.INT
      case _: Boolean => Schema.Type.BOOLEAN
      case _: Float => Schema.Type.FLOAT
      case _: Double => Schema.Type.DOUBLE
      case _: Array[Byte] => Schema.Type.BYTES
      case _: GenericData.EnumSymbol => Schema.Type.ENUM
      case other => other
    }
    val (first, rest) = schema.getTypes.asScala.partition(_.getType == defaultType)
    val result = Schema.createUnion(first.headOption.toSeq ++ rest: _*)
    schema.getObjectProps.asScala.foreach { case (k, v) => result.addProp(k, v) }
    result
  }

  /**
    * Requires a UNION schema and will move the null schema to the head if it exists.
    * Otherwise returns the UNION as is.
    */
  def moveNullToHead(schema: Schema): Schema = {
    require(schema.getType == Schema.Type.UNION, "Cannot order types in non union")
    val (nulls, rest) = schema.getTypes.asScala.partition(_.getType == Schema.Type.NULL)
    val result = Schema.createUnion(nulls.headOption.toSeq ++ rest: _*)
    schema.getAliases.asScala.foreach(result.addAlias)
    schema.getObjectProps.asScala.foreach { case (k, v) => result.addProp(k, v) }
    result
  }

  // creates a union schema type, with nested unions extracted, and duplicate nulls stripped
  // union schemas can't contain other union schemas as a direct
  // child, so whenever we create a union, we need to check if our
  // children are unions and flatten
  def createSafeUnion(schemas: Schema*): Schema = {
    val flattened = schemas.flatMap(schema => scala.util.Try(schema.getTypes.asScala).getOrElse(Seq(schema)))
    val (nulls, rest) = flattened.partition(_.getType == Schema.Type.NULL)
    Schema.createUnion(nulls.headOption.toSeq ++ rest: _*)
  }
}
