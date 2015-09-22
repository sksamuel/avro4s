package com.sksamuel.avro4s

import java.io.{File, InputStream}
import java.nio.file.{Paths, Files, Path}

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser

class ClassGenerator(schema: Schema) {

  import scala.collection.JavaConverters._

  def schemaToScalaType(schema: Schema): String = {
    schema.getType match {
      case Schema.Type.STRING => "String"
      case Schema.Type.BOOLEAN => "Boolean"
      case Schema.Type.DOUBLE => "Double"
      case Schema.Type.FLOAT => "Float"
      case Schema.Type.INT => "Int"
      case Schema.Type.LONG => "Long"
      case Schema.Type.RECORD => schema.getName
      case Schema.Type.ARRAY => s"Seq[${schemaToScalaType(schema.getElementType)}]"
      case Schema.Type.MAP => s"Map[String, ${schemaToScalaType(schema.getValueType)}]"
      case _ => "todo"
    }
  }

  def classForRecord(schema: Schema): ClassDef = {
    val fields = schema.getFields.asScala.map(field => FieldDef(field.name, schemaToScalaType(field.schema)))
    ClassDef(schema.getNamespace, schema.getName, fields)
  }

  def defs: Seq[ClassDef] = {
    def complex(schema: Schema): Option[Schema] = schema.getType match {
      case Schema.Type.RECORD => Some(schema)
      case Schema.Type.ARRAY => complex(schema.getElementType)
      case Schema.Type.MAP => complex(schema.getValueType)
      case _ => None
    }
    val seen = scala.collection.mutable.Set.empty[String]
    def records(schema: Schema): Seq[Schema] = {
      require(complex(schema).isDefined)
      if (seen.contains(schema.getFullName)) {
        Nil
      } else {
        seen.add(schema.getFullName)
        val rs = schema.getFields.asScala.map(f => complex(f.schema)).filter(_.isDefined).map(_.get)
        schema +: rs.flatMap(records)
      }
    }
    records(schema).map(classForRecord).distinct
  }
}

object ClassGenerator {
  def apply(in: InputStream): Seq[ClassDef] = new ClassGenerator(new Parser().parse(in)).defs
  def apply(path: Path): Seq[ClassDef] = apply(path.toFile)
  def apply(file: File): Seq[ClassDef] = new ClassGenerator(new Parser().parse(file)).defs
}

case class ClassDef(namespace: String, name: String, fields: Seq[FieldDef])

case class FieldDef(name: String, `type`: String)

object StringClassRenderer {

  def render(classes: Seq[ClassDef]): String = {
    require(classes.nonEmpty)
    s"package ${classes.head.namespace}\n\n" + classes.map(render).mkString("\n\n")
  }

  def render(classdef: ClassDef): String = {
    val isCase = if (classdef.fields.size <= 22) "case " else ""
    "// auto generated code by avro4s\n" +
      s"${isCase}class ${classdef.name}(\n" +
      classdef.fields.map(field => s"  ${field.name}: ${field.`type`}").mkString(",\n") +
      "\n)"
  }
}

object FileRenderer {
  def render(dir: Path, classes: Seq[ClassDef]): Seq[Path] = {
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