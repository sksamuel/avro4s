package com.sksamuel.avro4s

import java.io.{File, InputStream}
import java.nio.file.{Files, Path, Paths}

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser

class ClassGenerator(schema: Schema) {

  import scala.collection.JavaConverters._

  def records: Seq[Record] = {
    val records = scala.collection.mutable.Map.empty[String, Record]

    def schemaToType(schema: Schema): Type = {
      schema.getType match {
        case Schema.Type.ARRAY => ArrayType(schemaToType(schema.getElementType))
        case Schema.Type.BOOLEAN => Primitive("Boolean")
        case Schema.Type.DOUBLE => Primitive("Double")
        case Schema.Type.ENUM => EnumType(schema.getEnumSymbols.asScala)
        case Schema.Type.FIXED => Primitive("String")
        case Schema.Type.FLOAT => Primitive("Float")
        case Schema.Type.INT => Primitive("Int")
        case Schema.Type.LONG => Primitive("Long")
        case Schema.Type.MAP => MapType(schemaToType(schema.getValueType))
        case Schema.Type.RECORD => records.getOrElse(schema.getFullName, recordFor(schema))
        case Schema.Type.STRING => Primitive("String")
        case Schema.Type.UNION => Primitive("UNION")
        case _ => sys.error("Unsupported field type: " + schema.getType)
      }
    }

    def recordFor(schema: Schema): Record = {
      val record = Record(schema.getNamespace, schema.getName, Nil)
      records.put(schema.getFullName, record)
      val updated = record.copy(fields = schema.getFields.asScala.map { field =>
        FieldDef(field.name, schemaToType(field.schema))
      })
      records.put(schema.getFullName, updated)
      updated
    }

    require(schema.getType == Schema.Type.RECORD)
    recordFor(schema)
    records.values.toSeq
  }
}

object ClassGenerator {
  def apply(in: InputStream): Seq[Record] = new ClassGenerator(new Parser().parse(in)).records
  def apply(file: File): Seq[Record] = new ClassGenerator(new Parser().parse(file)).records
  def apply(path: Path): Seq[Record] = apply(path.toFile)
}

sealed trait Type

case class Record(namespace: String, name: String, fields: Seq[FieldDef]) extends Type

case class MapType(valueType: Type) extends Type

case class EnumType(values: Seq[String]) extends Type

case class Primitive(baseType: String) extends Type

case class ArrayType(arrayType: Type) extends Type

case class FieldDef(name: String, `type`: Type)


object StringClassRenderer {

  def render(classes: Seq[Record]): String = {
    require(classes.nonEmpty)
    s"package ${classes.head.namespace}\n\n" + classes.map(render).mkString("\n\n")
  }

  def render(classdef: Record): String = {
    val isCase = if (classdef.fields.size <= 22) "case " else ""
    "// auto generated code by avro4s\n" +
      s"${isCase}class ${classdef.name}(\n" +
      classdef.fields.map(field => s"  ${field.name}: ${field.`type`}").mkString(",\n") +
      "\n)"
  }
}

object FileRenderer {
  def render(dir: Path, classes: Seq[Record]): Seq[Path] = {
    classes.groupBy(_.namespace).map { case (namespace, packageClasses) =>
      val packageDir = dir.resolve(Paths.get(namespace.replace(".", File.separator)))
      packageDir.toFile.mkdirs()
      val path = packageDir.resolve(classes.head.name + ".scala")
      val writer = Files.newBufferedWriter(path)
      writer.write(StringClassRenderer.render(packageClasses))
      writer.close()
      path
    }.toList
  }
}