package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaFor.Typeclass
import magnolia.{SealedTrait, Subtype}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization.write
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.json4s.DefaultFormats

import scala.collection.JavaConverters._

sealed trait CustomDefault
case class CustomUnionDefault(className: String, values: java.util.Map[String, Any]) extends CustomDefault
case class CustomUnionWithEnumDefault(parentName: String, default: String, value: String) extends CustomDefault
case class CustomEnumDefault(value: String) extends CustomDefault

object CustomDefaults {

  implicit val formats = DefaultFormats

  def customScalaEnumDefault(value: Any) = CustomEnumDefault(value.toString)

  def customDefault(p: Product, schema: Schema): CustomDefault =
    if(isEnum(p, schema.getType))
      CustomEnumDefault(trimmedClassName(p))
    else {
      if(isUnionOfEnum(schema)) {
        val enumType = schema.getTypes.asScala.filter(_.getType == Schema.Type.ENUM).head
        CustomUnionWithEnumDefault(enumType.getName, trimmedClassName(p), p.toString)
      } else
        CustomUnionDefault(trimmedClassName(p), parse(write(p)).extract[Map[String, Any]].map {
          case (name, b: BigInt) if b.isValidInt => name -> b.intValue
          case (name, b: BigInt) if b.isValidLong => name -> b.longValue
          case (name, z) if schema.getType == Type.UNION => name ->
            schema.getTypes.asScala.find(_.getName == trimmedClassName(p)).map(_.getField(name).schema())
              .map(DefaultResolver(z, _)).getOrElse(z)
          case (name, z) => name -> DefaultResolver(z, schema.getField(name).schema())

        }.asJava)
    }

  def isUnionOfEnum(schema: Schema) = schema.getType == Schema.Type.UNION && schema.getTypes.asScala.map(_.getType).contains(Schema.Type.ENUM)

  def sealedTraitEnumDefaultValue[T](ctx: SealedTrait[Typeclass, T]) = {
    val defaultExtractor = new AnnotationExtractors(ctx.annotations)
    defaultExtractor.enumDefault.flatMap { default =>
      ctx.subtypes.flatMap { st: Subtype[Typeclass, T] =>
        if(st.typeName.short == default.toString)
          Option(st.typeName.short)
        else
          None
      }.headOption
    }
  }

  def isScalaEnumeration(value: Any) = value.getClass.getCanonicalName == "scala.Enumeration.Val"

  private def isEnum(product: Product, schemaType: Schema.Type) =
    product.productArity == 0 && schemaType == Schema.Type.ENUM

  private def trimmedClassName(p: Product) = trimDollar(p.getClass.getSimpleName)

  private def trimDollar(s: String) = if(s.endsWith("$")) s.dropRight(1) else s
}
