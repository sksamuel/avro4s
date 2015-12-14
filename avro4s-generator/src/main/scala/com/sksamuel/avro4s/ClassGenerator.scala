package com.sksamuel.avro4s

import java.io.{File, InputStream}
import java.nio.file.{Files, Path, Paths}

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser

class ClassGenerator(schema: Schema) {

  import scala.collection.JavaConverters._

  def records: Seq[Module] = {

    val types = scala.collection.mutable.Map.empty[String, Module]

    def schemaToType(schema: Schema): Type = {
      schema.getType match {
        case Schema.Type.ARRAY => ArrayType(schemaToType(schema.getElementType))
        case Schema.Type.BOOLEAN => PrimitiveType("Boolean")
        case Schema.Type.BYTES if schema.getProp("logicalType") == "decimal" => PrimitiveType("BigDecimal")
        case Schema.Type.BYTES => PrimitiveType("Array[Byte]")
        case Schema.Type.DOUBLE => PrimitiveType("Double")
        case Schema.Type.ENUM =>  types.getOrElse(schema.getFullName, enumFor(schema))
        case Schema.Type.FIXED => PrimitiveType("String")
        case Schema.Type.FLOAT => PrimitiveType("Float")
        case Schema.Type.INT => PrimitiveType("Int")
        case Schema.Type.LONG => PrimitiveType("Long")
        case Schema.Type.MAP => MapType(schemaToType(schema.getValueType))
        case Schema.Type.NULL => NullType
        case Schema.Type.RECORD => types.getOrElse(schema.getFullName, recordFor(schema))
        case Schema.Type.STRING => PrimitiveType("String")
        case Schema.Type.UNION => UnionType(schemaToType(schema.getTypes.get(0)), schemaToType(schema.getTypes.get(1)))
        case _ => sys.error("Unsupported field type: " + schema.getType)
      }
    }

    def enumFor(schema: Schema): EnumType = {
      val enum = EnumType(schema.getNamespace, schema.getName, schema.getEnumSymbols.asScala)
      types.put(schema.getFullName, enum)
      enum
    }

    def recordFor(schema: Schema): Record = {
      val record = Record(schema.getNamespace, schema.getName, Nil)
      types.put(schema.getFullName, record)
      val updated = record.copy(fields = schema.getFields.asScala.map { field =>
        FieldDef(field.name, schemaToType(field.schema))
      })
      types.put(schema.getFullName, updated)
      updated
    }

    require(schema.getType == Schema.Type.RECORD)
    recordFor(schema)
    types.values.toList
  }
}

object ClassGenerator {
  def apply(in: InputStream): Seq[Module] = new ClassGenerator(new Parser().parse(in)).records
  def apply(file: File): Seq[Module] = new ClassGenerator(new Parser().parse(file)).records
  def apply(path: Path): Seq[Module] = apply(path.toFile)
}

sealed trait Type

sealed trait Module extends Type {
  def namespace: String
  def name: String
}

case class Record(namespace: String, name: String, fields: Seq[FieldDef]) extends Module

case class EnumType(namespace: String, name: String, symbols: Seq[String]) extends Module

case class MapType(valueType: Type) extends Type

case class PrimitiveType(baseType: String) extends Type

case class ArrayType(arrayType: Type) extends Type

case class UnionType(left: Type, right: Type) extends Type

case object NullType extends Type

case class FieldDef(name: String, `type`: Type)

object TypeRenderer {
  def render(f: FieldDef): String = s"  ${f.name}: ${renderType(f.`type`)}"
  def renderType(t: Type): String = {
    t match {
      case PrimitiveType(base) => base
      case ArrayType(arrayType) => s"Seq[${renderType(arrayType)}]"
      case Record(namespace, name, _) => namespace + "." + name
      case EnumType(namespace, name, _) => namespace + "." + name
      case MapType(valueType) => s"Map[String, ${renderType(valueType)}]"
      case UnionType(NullType, right) => s"Option[${renderType(right)}]"
      case UnionType(left, right) => s"Either[${renderType(left)}, ${renderType(right)}]"
      case NullType => "null"
    }
  }
}

object ModuleRenderer {

  def render(modules: Seq[Module]): String = modules map {
    case record: Record => render(record)
    case enum: EnumType => render(enum)
  } mkString "\n\n"

  def renderRecords(records: Seq[Record]): String = {
    require(records.nonEmpty)
    s"package ${records.head.namespace}\n\n" + records.map(render).mkString("\n\n")
  }

  def render(record: Record): String = {
    "// auto generated code by avro4s\n" +
      s"case class ${record.name}(\n" + record.fields.map(TypeRenderer.render).mkString(",\n") + "\n)"
  }

  def render(enum: EnumType): String = {
    s"package ${enum.namespace}\n\n" + "// auto generated code by avro4s\n" + s"public enum ${enum.name}" + enum.symbols.mkString("{\n    ", ", ", "\n}")
  }
}

// templates contains all generated definitions grouped by file
case class Template(file: String, definition: String)

object TemplateRenderer {

  def render(modules: Seq[Module]): Seq[Template] = {
    val enums = modules.collect {
      case enum: EnumType => enum
    }

    val records = modules.collect {
      case record: Record => record
    }

    enums.map { enum =>
      Template(enum.namespace + "." + enum.name + ".java", ModuleRenderer.render(enum))
    } ++ records.groupBy(_.namespace).map { case (namespace, packageClasses) =>
      Template(namespace + "." + packageClasses.head.name + ".scala", ModuleRenderer.renderRecords(packageClasses))
    }
  }
}


object FileRenderer {
  def render(dir: Path, template: Seq[Template]): Seq[Path] = {

    template.map { template =>

      val targetPath = dir resolve Paths.get(template.file.replace(".", File.separator))
      val packagePath = targetPath.getParent
      packagePath.toFile.mkdirs()

      val writer = Files.newBufferedWriter(targetPath)
      writer.write(template.definition)
      writer.close()
      targetPath

    }
  }
}