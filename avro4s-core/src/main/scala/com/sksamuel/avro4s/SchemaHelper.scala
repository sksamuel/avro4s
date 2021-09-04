package com.sksamuel.avro4s

import org.apache.avro.{JsonProperties, Schema}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

import scala.util.matching.Regex

object SchemaHelper {

  import scala.collection.JavaConverters._

  def matchPrimitiveName(fullName: String): Option[Schema] = fullName match {
    case "java.lang.Integer" => Some(Schema.create(Schema.Type.INT))
    case "java.lang.String"  => Some(Schema.create(Schema.Type.STRING))
    case "java.lang.Long"    => Some(Schema.create(Schema.Type.LONG))
    case "java.lang.Boolean" => Some(Schema.create(Schema.Type.BOOLEAN))
    case "java.lang.Double"  => Some(Schema.create(Schema.Type.DOUBLE))
    case "java.lang.Float"   => Some(Schema.create(Schema.Type.FLOAT))
    case _                   => None
  }

  private val arrayTypeNamePattern: Regex = "scala.collection.immutable.::(__B)?".r

  def extractTraitSubschema(fullName: String, schema: Schema): Schema = matchPrimitiveName(fullName) getOrElse {
    if (schema.getType != Schema.Type.UNION)
      throw new Avro4sConfigurationException(s"Can only extract subschemas from a UNION but was given $schema")

    val types = schema.getTypes
    val size = types.size

    if (size == 0) throw new Avro4sConfigurationException(s"Cannot extract subschema from empty UNION $schema")

    // if we are looking for an array type then find "array" first
    // this is totally not FP but what the heck it's late and it's perfectly valid
    arrayTypeNamePattern.findFirstMatchIn(fullName) match {
      case Some(_) =>
        return types.asScala.find(_.getType == Schema.Type.ARRAY).getOrElse {
          throw new Avro4sConfigurationException(s"Could not find array type to match $fullName")
        }
      case None =>
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
      throw new Avro4sConfigurationException(s"Cannot find subschema for type [$fullName] in ${schema.getTypes}")
    }
  }

  /**
    * Requires a UNION schema and will attempt to find a subschema that
    * matches the default value type. If one matches it will move that
    * schema to the head, as required by the spec.
    */
  def moveDefaultToHead(schema: Schema, default: Any): Schema = {
    if(schema.getType != Schema.Type.UNION)
      throw new Avro4sConfigurationException(s"Can handle UNION schemas only, but got $schema")
    val defaultType = default match {
      case _: String                  => Schema.Type.STRING
      case x if x.getClass.isEnum     => Schema.Type.ENUM
      case _: Utf8                    => Schema.Type.STRING
      case _: Long                    => Schema.Type.LONG
      case _: Int                     => Schema.Type.INT
      case _: Boolean                 => Schema.Type.BOOLEAN
      case _: Float                   => Schema.Type.FLOAT
      case _: Double                  => Schema.Type.DOUBLE
      case _: Array[Byte]             => Schema.Type.BYTES
      case _: GenericData.EnumSymbol  => Schema.Type.ENUM
      case _: java.util.Map[_, _]     => Schema.Type.MAP
      case JsonProperties.NULL_VALUE  => Schema.Type.NULL
      case CustomEnumDefault(_)       => Schema.Type.ENUM
      case CustomArrayDefault(_)      => Schema.Type.ARRAY
      case other                      => other
    }

    val (first, rest) = schema.getTypes.asScala.partition { t =>
      defaultType match {
        case CustomUnionDefault(name, _) => name == t.getName
        case CustomUnionWithEnumDefault(name, default, _) =>
          name == t.getName
        case _ => t.getType == defaultType
      }
    }
    val result = Schema.createUnion(first.headOption.toSeq ++ rest: _*)
    schema.getObjectProps.asScala.foreach { case (k, v) => result.addProp(k, v) }
    result
  }

  /**
    * Requires a UNION schema and will move the null schema to the head if it exists.
    * Otherwise returns the UNION as is.
    */
  def moveNullToHead(schema: Schema): Schema = {
    if(schema.getType != Schema.Type.UNION)
      throw new Avro4sConfigurationException(s"Can order types only in UNION, but got $schema")
    if (schema.getTypes.asScala.exists(_.getType == Schema.Type.NULL)) {
      val (nulls, rest) = schema.getTypes.asScala.partition(_.getType == Schema.Type.NULL)
      val result = Schema.createUnion(nulls.headOption.toSeq ++ rest: _*)
      schema.getObjectProps.asScala.foreach { case (k, v) => result.addProp(k, v) }
      result
    } else schema
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

  /**
    * Takes an Avro schema, and overrides the namespace of that schema with the given namespace.
    */
  def overrideNamespace(schema: Schema, namespace: String): Schema = {
    schema.getType match {
      case Schema.Type.RECORD =>
        val fields = schema.getFields.asScala.map { field =>
          new Schema.Field(field.name(),
                           overrideNamespace(field.schema(), namespace),
                           field.doc,
                           field.defaultVal,
                           field.order)
        }
        val copy = Schema.createRecord(schema.getName, schema.getDoc, namespace, schema.isError, fields.asJava)
        schema.getAliases.asScala.foreach(copy.addAlias)
        schema.getObjectProps.asScala.foreach { case (k, v) => copy.addProp(k, v) }
        copy
      case Schema.Type.UNION => Schema.createUnion(schema.getTypes.asScala.map(overrideNamespace(_, namespace)).asJava)
      case Schema.Type.ENUM  => Schema.createEnum(schema.getName, schema.getDoc, namespace, schema.getEnumSymbols)
      case Schema.Type.FIXED => Schema.createFixed(schema.getName, schema.getDoc, namespace, schema.getFixedSize)
      case Schema.Type.MAP   => Schema.createMap(overrideNamespace(schema.getValueType, namespace))
      case Schema.Type.ARRAY => Schema.createArray(overrideNamespace(schema.getElementType, namespace))
      case _                 => schema
    }
  }
}
