package com.sksamuel.avro4s

import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization.write
import org.apache.avro.Schema
import org.json4s.DefaultFormats
import scala.collection.JavaConverters._

case class CustomUnionDefault(className: String, values: java.util.Map[String, Any])
case class CustomEnumDefault(value: String)

object CustomDefaultResolver {

  implicit val formats = DefaultFormats

  def customScalaEnumDefault(value: Any) = CustomEnumDefault(value.toString)

  def customUnionDefault(p: Product, schema: Schema) =
    if(isEnum(p, schema.getType))
      CustomEnumDefault(trimmedClassName(p))
    else
      CustomUnionDefault(trimmedClassName(p), parse(write(p)).extract[Map[String, Any]].asJava)

  private def isEnum(product: Product, schemaType: Schema.Type) =
    product.productArity == 0 && schemaType == Schema.Type.ENUM

  private def trimmedClassName(p: Product) = {
    val sn = p.getClass.getSimpleName
    if(sn.endsWith("$")) sn.dropRight(1) else sn
  }
}
