package com.sksamuel.avro4s

//import magnolia.{SealedTrait, Subtype}
import java.util.Map
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import scala.collection.JavaConverters.*

/** Extends [[java.util.Map]] for JSON serialization in [[org.apache.avro.util.internal.JacksonUtils.toJson]] */
case class CustomUnionDefault(className: String, value: java.util.Map[String, Any])
  extends java.util.AbstractMap[String, Any] {
  override def entrySet(): java.util.Set[java.util.Map.Entry[String, Any]] = value.entrySet()
}

/** Extends [[CharSequence]] for JSON serialization in [[org.apache.avro.util.internal.JacksonUtils.toJson]] */
case class CustomUnionWithEnumDefault(parentName: String, default: String, value: String)
  extends CharSequence {
  override def toString: String = value
  override def length: Int = value.length
  override def charAt(index: Int): Char = value.charAt(index)
  override def subSequence(start: Int, end: Int): CharSequence = value.subSequence(start, end)
}

object CustomDefaults {
//
//  def customScalaEnumDefault(value: Any) = CustomEnumDefault(value.toString)
//
  def customDefault(p: Product, schema: Schema): AnyRef =
    if (isEnum(p, schema.getType))
      trimmedClassName(p)
    else {
      if (isUnionOfEnum(schema)) {
        val enumType = schema.getTypes.asScala.filter(_.getType == Schema.Type.ENUM).head
        CustomUnionWithEnumDefault(enumType.getName, trimmedClassName(p), p.toString)
      } else
        CustomUnionDefault(trimmedClassName(p), p.productElementNames.zip(p.productIterator).map {
//          case (name, b: BigInt) if b.isValidInt => name -> b.intValue
//          case (name, b: BigInt) if b.isValidLong => name -> b.longValue
          case (name, z) if schema.getType == Type.UNION => name ->
            schema.getTypes.asScala.find(_.getName == trimmedClassName(p)).map(_.getField(name).schema())
              .map(DefaultResolver(z, _)).getOrElse(z)
          case (name, z) => name -> DefaultResolver(z, schema.getField(name).schema())

        }.toMap.asJava)
    }

  def isUnionOfEnum(schema: Schema) = schema.getType == Schema.Type.UNION && schema.getTypes.asScala.map(_.getType).contains(Schema.Type.ENUM)
//
//  def sealedTraitEnumDefaultValue[T](ctx: SealedTrait[SchemaFor, T]) = {
//    val defaultExtractor = new AnnotationExtractors(ctx.annotations)
//    defaultExtractor.enumDefault.flatMap { default =>
//      ctx.subtypes.flatMap { st: Subtype[SchemaFor, T] =>
//        if(st.typeName.short == default.toString)
//          Option(st.typeName.short)
//        else
//          None
//      }.headOption
//    }
//  }
//
//  def isScalaEnumeration(value: Any) = value.getClass.getCanonicalName == "scala.Enumeration.Val"
//
//
  private def isEnum(product: Product, schemaType: Schema.Type) =
    product.productArity == 0 && schemaType == Schema.Type.ENUM

  private def trimmedClassName(p: Product) = trimDollar(p.getClass.getSimpleName)

  private def trimDollar(s: String) = if(s.endsWith("$")) s.dropRight(1) else s
}
