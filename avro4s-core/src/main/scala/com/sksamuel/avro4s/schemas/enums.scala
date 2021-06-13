package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.avroutils.EnumHelpers
import com.sksamuel.avro4s.{AvroJavaEnumDefault, AvroJavaName, AvroJavaNamespace, AvroJavaProp, AvroName, AvroNameable, AvroNamespaceable, SchemaFor}
import org.apache.avro.{Schema, SchemaBuilder}

import scala.reflect.ClassTag

trait EnumSchemas:
  given[T <: Enum[_]](using tag: ClassTag[T]): SchemaFor[T] = new JavaEnumSchemaFor(tag)

//  object JavaEnumSchemaFor {
//
//    def apply[E <: Enum[_]](default: E)(using tag: ClassTag[E]): SchemaFor[E] =
//      com.sksamuel.avro4s.schemas.Enums.schema[E].map[E](EnumHelpers.addDefault(default))
//  }
//
//class JavaEnumWithDefault[T <: Enum[_]](tag: ClassTag[T]) extends SchemaFor[T] :

class JavaEnumSchemaFor[T <: Enum[_]](tag: ClassTag[T]) extends SchemaFor[T] :

  val symbols = tag.runtimeClass.getEnumConstants.map(_.toString)

  val maybeName = tag.runtimeClass.getAnnotations.collectFirst {
    case annotation: AvroJavaName => annotation.value()
    case annotation: AvroNameable => annotation.name
  }

  val maybeNamespace = tag.runtimeClass.getAnnotations.collectFirst {
    case annotation: AvroJavaNamespace => annotation.value()
    case annotation: AvroNamespaceable => annotation.namespace
  }

  val name = maybeName.getOrElse(tag.runtimeClass.getSimpleName) //(nameExtractor.name)
  val namespace = maybeNamespace.getOrElse(tag.runtimeClass.getPackage.getName) //.getOrElse(nameExtractor.namespace)

  val maybeEnumDefault = tag.runtimeClass.getDeclaredFields.collectFirst {
    case field if field.getDeclaredAnnotations.map(_.annotationType()).contains(classOf[AvroJavaEnumDefault]) =>
      field.getName
  }

  private val s = maybeEnumDefault
    .map { enumDefault =>
      SchemaBuilder.enumeration(name).namespace(namespace).defaultSymbol(enumDefault).symbols(symbols: _*)
    }
    .getOrElse {
      SchemaBuilder.enumeration(name).namespace(namespace).symbols(symbols: _*)
    }

  val props = tag.runtimeClass.getAnnotations.collect {
    case annotation: AvroJavaProp => annotation.key() -> annotation.value()
  }

  props.foreach {
    case (key, value) =>
      s.addProp(key, value)
  }

  override def schema: Schema = s
