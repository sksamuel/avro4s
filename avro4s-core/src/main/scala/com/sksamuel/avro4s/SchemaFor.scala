package com.sksamuel.avro4s

import com.sksamuel.avro4s.avroutils.{EnumHelpers, SchemaHelper}
import org.apache.avro.SchemaBuilder

import scala.reflect.ClassTag
//
///**
//  * A [[SchemaFor]] generates an Avro Schema for a Scala or Java type.
//  *
//  * For example, a String SchemaFor could return an instance of Schema.Type.STRING
//  * or Schema.Type.FIXED depending on the type required for Strings.
//
///**

object JavaEnumSchemaFor {

  def apply[E <: Enum[_]](default: E)(using tag: ClassTag[E]): SchemaFor[E] =
    com.sksamuel.avro4s.schemas.Enums.schema[E].map[E](EnumHelpers.addDefault(default))
}

//object ScalaEnumSchemaFor extends EnumSchemaFor {
//
//  def apply[E <: scala.Enumeration#Value](default: E)(implicit tag: TypeTag[E]): SchemaFor[E] =
//    SchemaFor.scalaEnumSchemaFor.map[E](EnumHelpers.addDefault(default))
//}

