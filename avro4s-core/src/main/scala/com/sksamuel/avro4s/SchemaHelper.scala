package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

object SchemaHelper {

  import scala.collection.JavaConverters._

  def matchPrimitiveName(fullName: String): Option[Schema] = fullName match {
    case "java.lang.Integer" => Some(Schema.create(Schema.Type.INT))
    case "java.lang.String" => Some(Schema.create(Schema.Type.STRING))
    case "java.lang.Long" => Some(Schema.create(Schema.Type.LONG))
    case "java.lang.Boolean" => Some(Schema.create(Schema.Type.BOOLEAN))
    case "java.lang.Double" => Some(Schema.create(Schema.Type.DOUBLE))
    case "java.lang.Float" => Some(Schema.create(Schema.Type.FLOAT))
    case _ => None
  }

  def extractTraitSubschema(fullName: String, schema: Schema): Schema = matchPrimitiveName(fullName) getOrElse {
    require(schema.getType == Schema.Type.UNION, s"Can only extract subschemas from a UNION but was given $schema")

    val types = schema.getTypes
    val size = types.size

    require(size > 0, s"Cannot extract subschema from empty UNION $schema")

    // if we are looking for an array type then find "array" first
    // this is totally not FP but what the heck it's late and it's perfectly valid
    fullName match {
      case "scala.collection.immutable.::" =>
        return types.asScala.find(_.getType == Schema.Type.ARRAY).getOrElse(sys.error(s"Could not find array type to match $fullName"))
      case _ =>
    }

    // Finds the matching schema and keeps track a null type if any.
    // If no matching schema is found in a union of size 2 the other type is returned, regardless of its name.
    // See https://github.com/sksamuel/avro4s/issues/268
    var result: Schema = null
    var nullIndex: Int = -1
    var i = 0
    do {
      val s = types.get(i)
      if (s.getFullName == fullName) {
        result = s
      } else if (s.getType == Schema.Type.NULL) {
        nullIndex = i
      }

      i = i + 1

    } while (i < size && result == null)

    if (result != null) { // Return the name based match
      result
    } else if (nullIndex != -1 && size == 2) { // Return the non null type.
      types.get(i - nullIndex)
    } else {
      sys.error(s"Cannot find subschema for type name $fullName in ${schema.getTypes}")
    }
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
