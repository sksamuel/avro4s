package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.{AvroJavaEnumDefault, AvroJavaName, AvroJavaNamespace, AvroJavaProp, AvroName, AvroNameable, AvroNamespaceable, SchemaFor}
import org.apache.avro.SchemaBuilder

import scala.reflect.ClassTag

trait EnumSchemas:
  given[T <: Enum[_]](using tag: ClassTag[T]): SchemaFor[T] = Enums.schema[T]

object Enums:
  def schema[T](using tag: ClassTag[T]): SchemaFor[T] =

    //    val typeInfo = TypeInfo.fromClass(tag.runtimeClass)
    //    val nameExtractor = Names()
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
    val namespace = maybeNamespace.getOrElse(tag.runtimeClass.getPackageName) //.getOrElse(nameExtractor.namespace)

    val maybeEnumDefault = tag.runtimeClass.getDeclaredFields.collectFirst {
      case field if field.getDeclaredAnnotations.map(_.annotationType()).contains(classOf[AvroJavaEnumDefault]) =>
        field.getName
    }

    val schema = maybeEnumDefault
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
        schema.addProp(key, value)
    }

    SchemaFor[T](schema)